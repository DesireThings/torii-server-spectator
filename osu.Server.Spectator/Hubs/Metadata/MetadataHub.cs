// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Primitives;
using Microsoft.Extensions.Logging;
using osu.Game.Online.Metadata;
using osu.Game.Users;
using osu.Server.QueueProcessor;
using osu.Server.Spectator.Authentication;
using osu.Server.Spectator.Database;
using osu.Server.Spectator.Database.Models;
using osu.Server.Spectator.Entities;
using osu.Server.Spectator.Extensions;
using osu.Server.Spectator.Hubs.Spectator;
using osu.Server.Spectator.Services;
using StackExchange.Redis;
using BeatmapUpdates = osu.Game.Online.Metadata.BeatmapUpdates;

namespace osu.Server.Spectator.Hubs.Metadata
{
    [Authorize(ConfigureJwtBearerOptions.LAZER_CLIENT_SCHEME)]
    public class MetadataHub : StatefulUserHub<IMetadataClient, MetadataClientState>, IMetadataServer
    {
        private readonly IMemoryCache cache;
        private readonly IDatabaseFactory databaseFactory;
        private readonly IDailyChallengeUpdater dailyChallengeUpdater;
        private readonly IScoreProcessedSubscriber scoreProcessedSubscriber;

        // Torii: cached map of "version hash → friendly client name (e.g. 'Torii Lazer')".
        // Resolved on every presence broadcast so other lazer clients can render a verified
        // badge next to your name. Refreshes itself in the background (see ToriiClientNameResolver),
        // so the lookup here is a constant-time dictionary read, not a network hop.
        private readonly ToriiClientNameResolver toriiClientNameResolver;

        // Torii: redis is used for cross-service presence (the g0v0 web/admin tooling reads
        // metadata:online:{userId} to figure out whether a user is currently connected to the
        // metadata hub without having to talk to SignalR directly). The key is set on connect
        // with a 2h TTL — long enough to survive a momentary disconnect, short enough to
        // self-clean if the spectator process disappears without firing OnDisconnectedAsync.
        private readonly IConnectionMultiplexer redis;

        // Torii: raw IHubContext used for the custom UserClientNameUpdated SignalR event.
        // The osu! lazer client subscribes to it via `connection.On<int, string?>("UserClientNameUpdated", ...)`
        // (see osu.Game.Online.Metadata.OnlineMetadataClient), but the IMetadataClient interface
        // upstream doesn't include the method, so we have to dispatch by name with raw SendAsync.
        private readonly IHubContext<MetadataHub> hubContext;

        internal const string ONLINE_PRESENCE_WATCHERS_GROUP = "metadata:online-presence-watchers";
        internal static string FRIEND_PRESENCE_WATCHERS_GROUP(int userId) => $"metadata:online-presence-watchers:{userId}";

        internal static string MultiplayerRoomWatchersGroup(long roomId) => $"metadata:multiplayer-room-watchers:{roomId}";

        public MetadataHub(
            ILoggerFactory loggerFactory,
            IMemoryCache cache,
            EntityStore<MetadataClientState> userStates,
            IDatabaseFactory databaseFactory,
            IDailyChallengeUpdater dailyChallengeUpdater,
            IScoreProcessedSubscriber scoreProcessedSubscriber,
            ToriiClientNameResolver toriiClientNameResolver,
            IConnectionMultiplexer redis,
            IHubContext<MetadataHub> hubContext)
            : base(loggerFactory, userStates)
        {
            this.cache = cache;
            this.databaseFactory = databaseFactory;
            this.dailyChallengeUpdater = dailyChallengeUpdater;
            this.scoreProcessedSubscriber = scoreProcessedSubscriber;
            this.toriiClientNameResolver = toriiClientNameResolver;
            this.redis = redis;
            this.hubContext = hubContext;
        }

        public override async Task OnConnectedAsync()
        {
            await base.OnConnectedAsync();

            using (var usage = await GetOrCreateLocalUserState())
            {
                usage.Item = new MetadataClientState(Context.ConnectionId, Context.GetUserId(), Context.GetVersionHash());

                await logLogin(usage);
                await Clients.Caller.DailyChallengeUpdated(dailyChallengeUpdater.Current);

                await refreshFriends(usage.Item);
            }
        }

        private async Task logLogin(ItemUsage<MetadataClientState> usage)
        {
            string? userIp = Context.GetHttpContext()?.Request.Headers.TryGetValue("X-Forwarded-For", out StringValues forwardedForIp) == true
                // header may contain multiple IPs by spec, first is usually what we care for.
                ? forwardedForIp.ToString().Split(',').First()
                // fallback to getting the raw IP.
                : Context.GetHttpContext()?.Connection.RemoteIpAddress?.ToString();

            // Torii: signal "this user has an active metadata connection" to the rest of the
            // stack via a redis key. The g0v0 backend reads this when answering /me requests
            // so the website can show a "currently online" badge without re-implementing
            // SignalR group introspection. 2h TTL self-cleans if the spectator dies without
            // firing OnDisconnectedAsync.
            redis.GetDatabase().StringSet($"metadata:online:{usage.Item!.UserId}", "metadata", TimeSpan.FromHours(2));

            using (var db = databaseFactory.GetInstance())
                await db.AddLoginForUserAsync(usage.Item.UserId, userIp);
        }

        public async Task<BeatmapUpdates> GetChangesSince(int queueId)
        {
            // Torii: g0v0 has no `bss_process_queue` table — beatmap submissions
            // round-trip through its own pipeline (`/api/v2/beatmaps/...`) and
            // notify clients via the SignalR `BeatmapSetsUpdated` broadcast
            // straight from the submission handler, so the client's
            // GetChangesSince poll has nothing to enumerate from this side.
            //
            // Without this short-circuit, every poll throws because the
            // upstream BeatmapStatusWatcher.GetUpdatedBeatmapSetsAsync tries
            // to query the missing table (and authenticates with separate
            // MYSQL_* env vars that aren't always wired). Returning an empty
            // delta with the same queue id the caller sent in is the same
            // behaviour the poller has when there's nothing new to report.
            if (!AppSettings.EnableBeatmapStatusPolling)
                return new BeatmapUpdates(Array.Empty<int>(), queueId);

            QueueProcessor.BeatmapUpdates updates = await BeatmapStatusWatcher.GetUpdatedBeatmapSetsAsync(queueId);
            return new BeatmapUpdates(updates.BeatmapSetIDs, updates.LastProcessedQueueID);
        }

        /// <summary>
        /// El fundador nunca figura desconectado: si no esta jugando de verdad, igual
        /// se lo muestra presente.
        /// </summary>
        /// <remarks>
        /// La presencia sintetica va con <c>Activity = null</c> A PROPOSITO. Un texto
        /// propio ("Watching my Grasshoppers") tendria que viajar como un
        /// <see cref="UserActivity"/> nuevo, y esa clase es parte del contrato
        /// messagepack del paquete compartido: agregarle un tipo obliga a republicar el
        /// paquete y a que cliente y server queden atados a la misma version, o sea que
        /// cualquier cliente viejo se rompe al recibirlo. Asi el cable no cambia y el
        /// texto lo pone el cliente, que es donde se dibuja igual.
        ///
        /// Por eso tambien "sin actividad" es la señal de que es la presencia sintetica:
        /// cuando el fundador esta de verdad adentro del juego SIEMPRE manda una
        /// actividad (eligiendo mapa, jugando, etc), asi que el cliente distingue los
        /// dos casos sin ningun campo nuevo.
        /// </remarks>
        private const int founder_user_id = 3;

        private static readonly UserPresence founder_idle_presence = new UserPresence
        {
            Status = UserStatus.Online,
            Activity = null,
        };

        public async Task BeginWatchingUserPresence()
        {
            var states = GetAllStates();
            foreach (var batch in states.Where(state => state.Value.UserStatus != UserStatus.Offline).Chunk(8))
            {
                await Task.WhenAll(batch.Select(async userState =>
                {
                    await Clients.Caller.UserPresenceUpdated(userState.Value.UserId, userState.Value.ToUserPresence());
                    string? clientName = toriiClientNameResolver.Resolve(userState.Value.VersionHash);
                    await hubContext.Clients.Client(Context.ConnectionId).SendAsync("UserClientNameUpdated", userState.Value.UserId, clientName);
                }));
            }

            // Si el fundador no estaba en la vuelta de arriba es porque no esta
            // conectado. Se lo manda igual: el que abre la lista de online lo tiene que
            // ver ahi, este jugando o no.
            if (!states.Any(state => state.Value.UserId == founder_user_id
                                             && state.Value.UserStatus != UserStatus.Offline))
            {
                await Clients.Caller.UserPresenceUpdated(founder_user_id, founder_idle_presence);
            }

            await Groups.AddToGroupAsync(Context.ConnectionId, ONLINE_PRESENCE_WATCHERS_GROUP);
        }

        public Task EndWatchingUserPresence()
            => Groups.RemoveFromGroupAsync(Context.ConnectionId, ONLINE_PRESENCE_WATCHERS_GROUP);

        public async Task UpdateActivity(UserActivity? activity)
        {
            using (var usage = await GetOrCreateLocalUserState())
            {
                Debug.Assert(usage.Item != null);

                if (usage.Item.UserActivity == null && activity == null)
                    return;

                usage.Item.UserActivity = activity;

                await Task.WhenAll
                (
                    shouldBroadcastPresenceToOtherUsers(usage.Item)
                        ? broadcastUserPresenceUpdate(usage.Item.UserId, usage.Item.ToUserPresence(), usage.Item.VersionHash)
                        : Task.CompletedTask,
                    Clients.Caller.UserPresenceUpdated(usage.Item.UserId, usage.Item.ToUserPresence())
                );
            }
        }

        public async Task UpdateStatus(UserStatus? status)
        {
            using (var usage = await GetOrCreateLocalUserState())
            {
                Debug.Assert(usage.Item != null);

                if (usage.Item.UserStatus == status)
                    return;

                usage.Item.UserStatus = status;

                await Task.WhenAll
                (
                    // Of note, we always send status updates to other users.
                    //
                    // This is a single special case where we don't check against `shouldBroadcastPresentToOtherUsers` because
                    // it is required to tell other clients that "we went offline" in the "appears offline" scenario.
                    broadcastUserPresenceUpdate(usage.Item.UserId, usage.Item.ToUserPresence(), usage.Item.VersionHash),
                    Clients.Caller.UserPresenceUpdated(usage.Item.UserId, usage.Item.ToUserPresence())
                );
            }

            switch (status)
            {
                case UserStatus.Online:
                case UserStatus.DoNotDisturb:
                    using (var db = databaseFactory.GetInstance())
                        await db.ToggleUserPresenceAsync(Context.GetUserId(), visible: true);
                    break;

                case UserStatus.Offline:
                    using (var db = databaseFactory.GetInstance())
                        await db.ToggleUserPresenceAsync(Context.GetUserId(), visible: false);
                    break;
            }
        }

        private static readonly object update_stats_lock = new object();

        public async Task<MultiplayerPlaylistItemStats[]> BeginWatchingMultiplayerRoom(long id)
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, MultiplayerRoomWatchersGroup(id));
            await scoreProcessedSubscriber.RegisterForMultiplayerRoomAsync(Context.GetUserId(), id);

            using var db = databaseFactory.GetInstance();

            MultiplayerRoomStats stats = (await cache.GetOrCreateAsync<MultiplayerRoomStats>($@"{nameof(MultiplayerRoomStats)}#{id}", e =>
            {
                e.SlidingExpiration = TimeSpan.FromHours(24);
                return Task.FromResult(new MultiplayerRoomStats { RoomID = id });
            }))!;

            await updateMultiplayerRoomStatsAsync(db, stats);

            // Outside of locking so may be mid-update, but that's fine we don't need perfectly accurate for client-side.
            return stats.PlaylistItemStats.Values.ToArray();
        }

        private async Task updateMultiplayerRoomStatsAsync(IDatabaseAccess db, MultiplayerRoomStats stats)
        {
            long[] playlistItemIds = (await db.GetAllPlaylistItemsAsync(stats.RoomID)).Select(item => item.id).ToArray();

            for (int i = 0; i < playlistItemIds.Length; ++i)
            {
                long itemId = playlistItemIds[i];

                if (!stats.PlaylistItemStats.TryGetValue(itemId, out var itemStats))
                    stats.PlaylistItemStats[itemId] = itemStats = new MultiplayerPlaylistItemStats { PlaylistItemID = itemId, };

                ulong lastProcessed = itemStats.LastProcessedScoreID;

                // Torii: g0v0's GetPassingScoresForPlaylistItem joins on the room id
                // (it reads from `playlist_best_scores` rather than upstream's score-table
                // sweep, and that table's primary key includes room_id).
                SoloScore[] scores = (await db.GetPassingScoresForPlaylistItem(stats.RoomID, itemId, itemStats.LastProcessedScoreID)).ToArray();

                if (scores.Length == 0)
                    return;

                // Lock globally for simplicity.
                // If it ever becomes an issue we can move to per-item locking or something more complex.
                lock (update_stats_lock)
                {
                    // check whether last id has changed since database query completed. if it did, this means another run would have updated the stats.
                    // for simplicity, just skip the update and wait for the next.
                    if (lastProcessed == itemStats.LastProcessedScoreID)
                    {
                        Dictionary<int, long> totals = scores
                                                       .Select(s => s.total_score)
                                                       .GroupBy(score => (int)Math.Clamp(Math.Floor((float)score / 100000), 0, MultiplayerPlaylistItemStats.TOTAL_SCORE_DISTRIBUTION_BINS - 1))
                                                       .OrderBy(grp => grp.Key)
                                                       .ToDictionary(grp => grp.Key, grp => grp.LongCount());

                        itemStats.CumulativeScore += scores.Sum(s => s.total_score);
                        for (int j = 0; j < MultiplayerPlaylistItemStats.TOTAL_SCORE_DISTRIBUTION_BINS; j++)
                            itemStats.TotalScoreDistribution[j] += totals.GetValueOrDefault(j);
                        itemStats.LastProcessedScoreID = scores.Max(s => s.id);
                    }
                }
            }
        }

        public async Task EndWatchingMultiplayerRoom(long id)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, MultiplayerRoomWatchersGroup(id));
            await scoreProcessedSubscriber.UnregisterFromMultiplayerRoomAsync(Context.GetUserId(), id);
        }

        public async Task RefreshFriends()
        {
            using (var usage = await GetOrCreateLocalUserState())
            {
                Debug.Assert(usage.Item != null);
                await refreshFriends(usage.Item);
            }
        }

        private async Task refreshFriends(MetadataClientState state)
        {
            // Remove the caller from any friend tracking groups.
            foreach (int friendId in state.FriendIds)
                await Groups.RemoveFromGroupAsync(Context.ConnectionId, FRIEND_PRESENCE_WATCHERS_GROUP(friendId));

            int[] newFriendIds;

            using (var db = databaseFactory.GetInstance())
            {
                newFriendIds = (await db.GetUserFriendsAsync(state.UserId))
                               // Once upon a time users were able to add themselves as friends.
                               // This errors during the state retrieval below, so let's not support it.
                               .Where(u => u != state.UserId)
                               .ToArray();
            }

            // Add the caller to the friend tracking groups.
            foreach (int friendId in newFriendIds)
                await Groups.AddToGroupAsync(Context.ConnectionId, FRIEND_PRESENCE_WATCHERS_GROUP(friendId));

            // Broadcast presence from any online friends to the caller.
            foreach (int friendId in newFriendIds.Except(state.FriendIds))
            {
                using (var friendUsage = await TryGetStateFromUser(friendId))
                {
                    if (friendUsage?.Item != null && shouldBroadcastPresenceToOtherUsers(friendUsage.Item))
                        await Clients.Caller.FriendPresenceUpdated(friendId, friendUsage.Item.ToUserPresence());
                }
            }

            state.FriendIds = newFriendIds;
        }

        protected override async Task CleanUpState(ItemUsage<MetadataClientState> state)
        {
            Debug.Assert(state.Item != null);

            await base.CleanUpState(state);

            // Torii: tear down the cross-service presence signals we set up in logLogin.
            //   1. Drop the `metadata:online:{userId}` redis key so the website /
            //      admin tooling immediately knows this user is no longer reachable
            //      via the metadata hub.
            //   2. Update `lazer_users.last_visit` so profile pages render "last seen X ago"
            //      correctly even if the client crashes without firing UpdateStatus(Offline).
            // El fundador queda prendido tambien aca. Si se apagara solo en el juego, el
            // perfil de la web diria "last seen hace un rato" mientras la lista de online
            // lo muestra presente, que es peor que cualquiera de las dos cosas sola.
            if (state.Item.UserId != founder_user_id)
            {
                redis.GetDatabase().KeyDelete($"metadata:online:{state.Item.UserId}");
                using (var db = databaseFactory.GetInstance())
                    await db.OfflineUser(state.Item.UserId);
            }

            if (shouldBroadcastPresenceToOtherUsers(state.Item))
                await broadcastUserPresenceUpdate(state.Item.UserId, null, state.Item.VersionHash);
            await scoreProcessedSubscriber.UnregisterFromAllMultiplayerRoomsAsync(state.Item.UserId);
        }

        // versionHash is passed in from the caller (which already holds the
        // MetadataClientState lock for the given userId) rather than re-resolved
        // here via TryGetStateFromUser, because re-acquiring the same EntityStore
        // lock on the same thread that's already holding it deadlocks (the lock
        // isn't reentrant). Symptom before the refactor was every UpdateActivity
        // / UpdateStatus / CleanUpState call timing out with
        // "Lock for MetadataClientState id N could not be obtained within timeout
        // period" and CreateRoom etc. inheriting the timeout.
        private Task broadcastUserPresenceUpdate(int userId, UserPresence? userPresence, string? versionHash)
        {
            // we never want appearing offline users to have their status broadcast to other clients.
            Debug.Assert(userPresence?.Status != UserStatus.Offline);

            // El fundador no se apaga. Va aca y no en el desconectar porque por esta
            // funcion pasan TODOS los avisos de presencia: cubrir un solo camino dejaria
            // los otros mandando null y lo apagarian igual.
            if (userId == founder_user_id && userPresence == null)
                userPresence = founder_idle_presence;

            // Torii: alongside every presence update, broadcast the verified-Torii client name
            // (or null for vanilla / unverified clients). Receivers stash it in a side-table
            // so the username chip can render the "Torii" badge next to verified players.
            string? clientName = toriiClientNameResolver.Resolve(versionHash);

            return Task.WhenAll
            (
                Clients.Group(ONLINE_PRESENCE_WATCHERS_GROUP).UserPresenceUpdated(userId, userPresence),
                Clients.Group(FRIEND_PRESENCE_WATCHERS_GROUP(userId)).FriendPresenceUpdated(userId, userPresence),
                hubContext.Clients.Group(ONLINE_PRESENCE_WATCHERS_GROUP).SendAsync("UserClientNameUpdated", userId, clientName),
                hubContext.Clients.Group(FRIEND_PRESENCE_WATCHERS_GROUP(userId)).SendAsync("UserClientNameUpdated", userId, clientName)
            );
        }

        private bool shouldBroadcastPresenceToOtherUsers(MetadataClientState state)
        {
            if (state.UserStatus == null)
                return false;

            switch (state.UserStatus.Value)
            {
                case UserStatus.Offline:
                    return false;

                case UserStatus.DoNotDisturb:
                case UserStatus.Online:
                    return true;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}
