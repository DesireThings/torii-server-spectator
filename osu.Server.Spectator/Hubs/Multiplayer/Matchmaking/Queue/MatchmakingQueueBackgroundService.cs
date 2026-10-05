// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Linq;
using osu.Server.Spectator.Hubs.Multiplayer.Matchmaking.RankedPlay;
using osu.Game.Online.Multiplayer.MatchTypes.RankedPlay;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using osu.Game.Online.API;
using osu.Game.Online.Matchmaking;
using osu.Game.Online.Matchmaking.Requests;
using osu.Game.Online.Matchmaking.Responses;
using osu.Game.Online.Multiplayer;
using osu.Server.Spectator.Database;
using osu.Server.Spectator.Database.Models;
using osu.Server.Spectator.Hubs.Multiplayer.Matchmaking.Elo;
using osu.Server.Spectator.Entities;
using osu.Server.Spectator.Services;
using StatsdClient;

namespace osu.Server.Spectator.Hubs.Multiplayer.Matchmaking.Queue
{
    public class MatchmakingQueueBackgroundService : BackgroundService, IMatchmakingQueueBackgroundService
    {
        /// <summary>
        /// The rate at which the matchmaking queue is updated.
        /// </summary>
        private static readonly TimeSpan queue_update_rate = AppSettings.MatchmakingQueueUpdateRate;

        /// <summary>
        /// The rate at which users are sent lobby status updates.
        /// </summary>
        private static readonly TimeSpan lobby_update_rate = AppSettings.MatchmakingLobbyUpdateRate;

        private const string statsd_prefix = "matchmaking";
        private static string queue_ban_start_time(int userId) => $"matchmaking-ban-start-time:{userId}";

        private readonly ConcurrentDictionary<int, MatchmakingLobby> poolLobbies = new ConcurrentDictionary<int, MatchmakingLobby>();
        private readonly ConcurrentDictionary<int, MatchmakingQueue> poolQueues = new ConcurrentDictionary<int, MatchmakingQueue>();
        private readonly ConcurrentDictionary<Guid, MatchmakingQueue> duelQueues = new ConcurrentDictionary<Guid, MatchmakingQueue>();
        private readonly ConcurrentDictionary<uint, MatchmakingBeatmapSelector> poolSelectors = new ConcurrentDictionary<uint, MatchmakingBeatmapSelector>();

        private readonly IHubContext<MultiplayerHub> hub;
        private readonly ISharedInterop sharedInterop;
        private readonly IDatabaseFactory databaseFactory;
        private readonly EntityStore<ServerMultiplayerRoom> rooms;
        private readonly IMultiplayerRoomController roomController;
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger logger;
        private readonly IMemoryCache memoryCache;
        private readonly MultiplayerEventDispatcher eventDispatcher;

        private DateTimeOffset lastLobbyUpdateTime = DateTimeOffset.UnixEpoch;
        private DateTimeOffset lastQueueRefreshTime = DateTimeOffset.UnixEpoch;
        private DateTimeOffset lastPoolUpdateTime = DateTimeOffset.UnixEpoch;
        private DateTimeOffset lastPoolRefreshTime = DateTimeOffset.UnixEpoch;

        public MatchmakingQueueBackgroundService(IHubContext<MultiplayerHub> hub, ISharedInterop sharedInterop, IDatabaseFactory databaseFactory, ILoggerFactory loggerFactory,
                                                 EntityStore<ServerMultiplayerRoom> rooms, IMultiplayerRoomController roomController, IMemoryCache memoryCache,
                                                 MultiplayerEventDispatcher eventDispatcher)
        {
            this.hub = hub;
            this.sharedInterop = sharedInterop;
            this.databaseFactory = databaseFactory;
            this.rooms = rooms;
            this.roomController = roomController;
            this.memoryCache = memoryCache;
            this.eventDispatcher = eventDispatcher;

            this.loggerFactory = loggerFactory;
            logger = loggerFactory.CreateLogger(nameof(MatchmakingQueueBackgroundService));
        }

        /// <summary>
        /// Manda el conteo de partidas en curso YA, sin esperar la tanda de 5 segundos.
        /// </summary>
        /// <remarks>
        /// Se llama cuando arranca o termina una partida. Sin esto, dos personas
        /// desaparecian de la cola y el punto verde tardaba hasta cinco segundos en
        /// aparecer: en el medio la pildora se veia apagada, como si se hubieran
        /// esfumado. El momento en que la gente MIRA es justo ese.
        /// </remarks>
        public async Task PushLiveMatchCountAsync(int poolId)
        {
            try
            {
                await hub.Clients.Group($"matchmaking-lobby-users:{poolId}")
                         .SendAsync("RankedPlayLiveMatchCount", poolId, contarPartidasEnCurso(poolId));
            }
            catch
            {
                // Es un aviso cosmetico. Si falla, la tanda de 5 segundos lo corrige.
            }
        }

        private int contarPartidasEnCurso(int poolId)
        {
            int n = 0;

            foreach ((_, ServerMultiplayerRoom? room) in rooms.GetAllEntities())
            {
                if (room?.MatchController is not RankedPlayMatchController rp)
                    continue;

                if (rp.PoolId != (uint)poolId)
                    continue;

                if (rp.State.Stage == RankedPlayStage.WaitForJoin || rp.State.Stage == RankedPlayStage.Ended)
                    continue;

                n++;
            }

            return n;
        }

        private Dictionary<uint, int> getLiveMatchCounts()
        {
            var counts = new Dictionary<uint, int>();
            foreach ((_, ServerMultiplayerRoom? room) in rooms.GetAllEntities())
            {
                if (room?.MatchController is not RankedPlayMatchController rp)
                    continue;

                if (rp.State.Stage == RankedPlayStage.WaitForJoin || rp.State.Stage == RankedPlayStage.Ended)
                    continue;

                counts[rp.PoolId] = counts.GetValueOrDefault(rp.PoolId) + 1;
            }

            return counts;
        }

        public async Task<string> GetLiveMatchesJsonAsync(int poolId)
        {
            var salidas = new List<object>();

            // GetAllEntities da una foto sin tomar el lock de cada sala. Alcanza: esto
            // alimenta un panel informativo, y una vida un frame vieja no le importa a
            // nadie. Tomar el lock de cada sala para dibujar una barrita seria pelear
            // con el hilo que las esta corriendo.
            foreach ((long roomId, ServerMultiplayerRoom? room) in rooms.GetAllEntities())
            {
                if (room?.MatchController is not RankedPlayMatchController rp)
                    continue;

                if (rp.PoolId != (uint)poolId)
                    continue;

                // Una sala que todavia espera gente no es una partida: mostrarla como
                // "en curso" haria que el panel prometa algo que no esta pasando.
                if (rp.State.Stage == RankedPlayStage.WaitForJoin || rp.State.Stage == RankedPlayStage.Ended)
                    continue;

                var jugadores = new List<object>();

                foreach ((int userId, RankedPlayUserInfo info) in rp.State.Users)
                {
                    MultiplayerRoomUser? u = room.Users.FirstOrDefault(x => x.UserID == userId);

                    // El username puede venir vacio: la sala guarda el APIUser solo si
                    // se hidrato, y en muchos casos no. Se manda vacio y lo resuelve el
                    // cliente contra su cache de usuarios, que es donde ese dato vive de
                    // verdad. Antes se inventaba "User 96", que es peor que no decir
                    // nada: parece un nombre.
                    jugadores.Add(new
                    {
                        user_id = userId,
                        username = u?.User?.Username ?? string.Empty,
                        life = info.Life,
                        max_life = 1_000_000,
                        rating = info.Rating,
                    });
                }

                bool jugando = rp.State.Stage == RankedPlayStage.Gameplay;
                int? beatmapId = null;

                try
                {
                    // Solo el ID. El titulo lo resuelve el cliente, que ya tiene cache de
                    // mapas: database_beatmap aca no trae artista ni titulo, y agregar un
                    // JOIN para dibujar un renglon no vale la pena.
                    beatmapId = rp.CurrentItem.BeatmapID;
                }
                catch
                {
                    // Sin item elegido todavia. El panel muestra la fase igual.
                }

                salidas.Add(new
                {
                    room_id = roomId,
                    players = jugadores,
                    stage = describirEtapa(rp.State.Stage),
                    round = rp.State.CurrentRound,
                    beatmap_id = beatmapId,
                    in_gameplay = jugando,
                });
            }

            return JsonConvert.SerializeObject(salidas);
        }

        /// <summary>
        /// El nombre de la etapa como lo lee un jugador, no como lo escribe el enum.
        /// </summary>
        private static string describirEtapa(RankedPlayStage stage) => stage switch
        {
            RankedPlayStage.RoundWarmup => "Getting ready",
            RankedPlayStage.CardDiscard => "Swapping cards",
            RankedPlayStage.FinishCardDiscard => "Swapping cards",
            RankedPlayStage.FinishCardPlay => "Picking a card",
            RankedPlayStage.GameplayWarmup => "Getting ready",
            RankedPlayStage.CardPlay => "Picking a card",
            RankedPlayStage.Gameplay => "Playing",
            RankedPlayStage.Results => "Round results",
            _ => stage.ToString(),
        };

        public Task RecordMatch(int poolId, MatchRoomState status)
        {
            if (!poolLobbies.TryGetValue(poolId, out MatchmakingLobby? lobby))
                return Task.CompletedTask;

            lobby.RecordMatch(status);
            return Task.CompletedTask;
        }

        public async Task RecordBeatmapResult(uint poolId, int beatmapId, APIMod[] mods, int[] scores, EloRating[] ratings)
        {
            if (!poolQueues.TryGetValue((int)poolId, out MatchmakingQueue? queue))
                return;

            try
            {
                if (!poolSelectors.TryGetValue(poolId, out MatchmakingBeatmapSelector? selector))
                    poolSelectors[poolId] = selector = await MatchmakingBeatmapSelector.Initialise(queue.Pool, databaseFactory);

                await selector.AdjustRating(new MatchmakingBeatmapSelector.BeatmapLookupKey(beatmapId, mods.Length == 0 ? "[]" : JsonConvert.SerializeObject(mods)), scores, ratings);
            }
            catch (Exception ex)
            {
                // torii: el ajuste de rating del mapa es CALIBRACION de dificultad, no afecta el
                // resultado del match (el ELO de los jugadores se calcula aparte en el controller).
                // Si algo aca tira (Initialise, o QuerySingleOrDefault con filas duplicadas ahora que
                // el CAST matchea de verdad) logueamos y tragamos: NUNCA debe subir a ResultsStage.
                logger.LogError(ex, "AdjustRating/Initialise del pool {PoolId} beatmap {BeatmapId} tiro; salteo la calibracion de rating de ese mapa", poolId, beatmapId);
            }
        }

        public bool IsInQueue(MultiplayerClientState state)
        {
            foreach ((_, MatchmakingQueue queue) in poolQueues)
            {
                if (queue.IsInQueue(new MatchmakingQueueUser(state.ConnectionId)))
                    return true;
            }

            return false;
        }

        public async Task AddToLobbyAsync(MultiplayerClientState state, int poolId)
        {
            // Users should only ever be in one lobby at a time.
            await RemoveFromLobbyAsync(state);

            using (var db = databaseFactory.GetInstance())
            {
                matchmaking_pool pool = await db.GetMatchmakingPoolAsync((uint)poolId) ?? throw new InvalidStateException($"Pool not found: {poolId}");

                if (!pool.active)
                    throw new InvalidStateException("The selected matchmaking pool is no longer active.");

                MatchmakingLobby lobby = poolLobbies.GetOrAdd(poolId, _ => new MatchmakingLobby(poolId, hub, databaseFactory)
                {
                    LookupQueue = id => poolQueues.GetValueOrDefault(id)
                });

                await lobby.Add(state);
            }
        }

        public async Task RemoveFromLobbyAsync(MultiplayerClientState state)
        {
            foreach ((_, MatchmakingLobby lobby) in poolLobbies)
                await lobby.Remove(state);
        }

        public async Task AddToQueueAsync(MultiplayerClientState state, int poolId)
        {
            // Users should only ever be in one queue at a time.
            await RemoveFromQueueAsync(state);

            using (var db = databaseFactory.GetInstance())
            {
                matchmaking_pool pool = await db.GetMatchmakingPoolAsync((uint)poolId) ?? throw new InvalidStateException($"Pool not found: {poolId}");

                if (!pool.active)
                    throw new InvalidStateException("The selected matchmaking pool is no longer active.");

                MatchmakingQueue queue = poolQueues.GetOrAdd(poolId, _ => new MatchmakingQueue(pool));
                await processBundle(queue.Add(await createUserAsync(state, pool)));
            }
        }

        public async Task RemoveFromQueueAsync(MultiplayerClientState state)
        {
            foreach ((_, MatchmakingQueue queue) in poolQueues)
                await processBundle(queue.Remove(new MatchmakingQueueUser(state.ConnectionId)));

            foreach ((_, MatchmakingQueue queue) in duelQueues)
                await processBundle(queue.Remove(new MatchmakingQueueUser(state.ConnectionId)));
        }

        public async Task<MatchmakingIssueDuelResponse> IssueDuelAsync(MultiplayerClientState state, MatchmakingIssueDuelRequest request)
        {
            // Users should only ever be in one queue at a time.
            await RemoveFromQueueAsync(state);

            using (var db = databaseFactory.GetInstance())
            {
                matchmaking_pool pool = await db.GetMatchmakingPoolAsync((uint)request.PoolId) ?? throw new InvalidStateException($"Pool not found: {request.PoolId}");
                pool.lobby_size = 2;
                pool.rating_search_radius = int.MaxValue;
                pool.ranked = false;

                if (!pool.active)
                    throw new InvalidStateException("The selected matchmaking pool is no longer active.");

                // The user is added to the queue before the queue is added to the dictionary
                // so that the periodic update doesn't discard the queue due to a lack of users.
                // EnforceStarSpread apagado a proposito: en un duelo elegis a quien
                // desafias. El limite existe para que la COLA no te empareje con
                // cualquiera, no para prohibirte jugar con un amigo de otra dificultad.
                MatchmakingQueue queue = new MatchmakingQueue(pool) { SearchTimeout = TimeSpan.FromMinutes(5), EnforceStarSpread = false };
                MatchmakingQueueUser user = await createUserAsync(state, pool);
                user.QueueBanStartTime = DateTimeOffset.MinValue;
                MatchmakingQueueUpdateBundle updateBundle = queue.Add(user);

                Guid duelGuid = Guid.NewGuid();
                if (!duelQueues.TryAdd(duelGuid, queue))
                    throw new InvalidStateException("Failed to issue the duel.");

                await processBundle(updateBundle);

                await hub.Clients.User(request.UserId.ToString()).SendAsync(nameof(IMatchmakingClient.MatchmakingDuelIssued), new MatchmakingDuelIssuedParams
                {
                    Id = duelGuid,
                    Pool = pool.ToMatchmakingPool(),
                    UserId = state.UserId
                });

                return new MatchmakingIssueDuelResponse();
            }
        }

        public async Task<MatchmakingAcceptDuelResponse> AcceptDuelAsync(MultiplayerClientState state, MatchmakingAcceptDuelRequest request)
        {
            // This could happen if the challenger cancelled the request. In which case, return an empty success.
            if (!duelQueues.TryGetValue(request.Id, out MatchmakingQueue? queue))
                return new MatchmakingAcceptDuelResponse();

            // Remove the user from all matchmaking queues.
            foreach ((_, MatchmakingQueue q) in poolQueues)
                await processBundle(q.Remove(new MatchmakingQueueUser(state.ConnectionId)));

            // Remove the user from all other duel queues.
            foreach ((_, MatchmakingQueue q) in duelQueues)
            {
                if (q != queue)
                    await processBundle(q.Remove(new MatchmakingQueueUser(state.ConnectionId)));
            }

            // Add the user to the duel queue.
            MatchmakingQueueUser user = await createUserAsync(state, queue.Pool);
            user.QueueBanStartTime = DateTimeOffset.MinValue;
            await processBundle(queue.Add(user));

            return new MatchmakingAcceptDuelResponse();
        }

        public async Task AcceptInvitationAsync(MultiplayerClientState state)
        {
            // Immediately notify the incoming user of their intent to join the match.
            await hub.Clients.Client(state.ConnectionId).SendAsync(nameof(IMatchmakingClient.MatchmakingQueueStatusChanged), new MatchmakingQueueStatus.JoiningMatch());

            foreach ((_, MatchmakingQueue queue) in poolQueues)
                await processBundle(queue.MarkInvitationAccepted(new MatchmakingQueueUser(state.ConnectionId)));

            foreach ((_, MatchmakingQueue queue) in duelQueues)
                await processBundle(queue.MarkInvitationAccepted(new MatchmakingQueueUser(state.ConnectionId)));
        }

        public async Task DeclineInvitationAsync(MultiplayerClientState state)
        {
            foreach ((_, MatchmakingQueue queue) in poolQueues)
                await processBundle(queue.MarkInvitationDeclined(new MatchmakingQueueUser(state.ConnectionId)));

            foreach ((_, MatchmakingQueue queue) in duelQueues)
                await processBundle(queue.MarkInvitationDeclined(new MatchmakingQueueUser(state.ConnectionId)));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ExecuteOnceAsync();
                }
                catch (Exception ex)
                {
                    // red de seguridad final: una excepcion de un solo tick del matchmaking
                    // JAMAS debe propagar al host (StopHost reinicia el server multiplayer
                    // entero y mata todos los matches). se logea y el loop sigue vivo.
                    logger.LogError(ex, "Unhandled exception in matchmaking queue tick.");
                }

                await Task.Delay(queue_update_rate, stoppingToken);
            }
        }

        /// <summary>
        /// Executes a single update of the queues.
        /// </summary>
        public async Task ExecuteOnceAsync()
        {
            try
            {
                await updateLobbies();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to update the matchmaking lobby.");
            }

            try
            {
                await refreshQueues();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to refresh the matchmaking queue.");
            }

            try
            {
                await refreshPools();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to refresh the matchmaking pools.");
            }

            foreach ((_, MatchmakingQueue queue) in poolQueues)
            {
                DogStatsd.Gauge($"{statsd_prefix}.queue.users", queue.Count, tags: [$"queue:{queue.Pool.DisplayName}"]);

                foreach (var user in queue.GetAllUsers())
                    DogStatsd.Histogram($"{statsd_prefix}.queue.users.rating", user.Rating.Mu, tags: [$"queue:{queue.Pool.DisplayName}"]);

                try
                {
                    await processBundle(queue.Update());
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to update the matchmaking queue for pool {poolId}.", queue.Pool.id);
                }
            }

            foreach ((Guid duelGuid, MatchmakingQueue queue) in duelQueues.ToArray())
            {
                if (queue.Count == 0)
                    duelQueues.Remove(duelGuid, out _);
                else
                    await processBundle(queue.Update());
            }
        }

        private async Task updateLobbies()
        {
            if (DateTimeOffset.Now - lastLobbyUpdateTime < lobby_update_rate)
                return;

            Dictionary<uint, int>? liveMatchCounts = null;
            foreach ((int poolId, MatchmakingLobby lobby) in poolLobbies)
            {
                await lobby.Update();
                liveMatchCounts ??= getLiveMatchCounts();

                // El conteo de partidas en curso va PEGADO al update del lobby, en la
                // misma tanda de cada 5 segundos, y no como stream propio: es un entero,
                // los que lo reciben son los mismos, y abrir un segundo canal para eso
                // seria duplicar trafico para no ganar nada.
                //
                // Por nombre de metodo asi es aditivo: un cliente que no lo conoce
                // simplemente no lo escucha.
                int enCurso = liveMatchCounts.GetValueOrDefault((uint)poolId);

                await hub.Clients.Group($"matchmaking-lobby-users:{poolId}")
                         .SendAsync("RankedPlayLiveMatchCount", poolId, enCurso);
            }

            lastLobbyUpdateTime = DateTimeOffset.Now;
        }

        private async Task refreshQueues()
        {
            if (DateTimeOffset.Now - lastQueueRefreshTime < TimeSpan.FromMinutes(1))
                return;

            using (var db = databaseFactory.GetInstance())
            {
                foreach ((_, MatchmakingQueue queue) in poolQueues)
                {
                    matchmaking_pool? newPool = await db.GetMatchmakingPoolAsync(queue.Pool.id);

                    if (newPool?.active != true)
                        await processBundle(queue.Clear());
                    else
                        queue.Refresh(newPool);
                }
            }

            lastQueueRefreshTime = DateTimeOffset.Now;
        }

        private async Task refreshPools()
        {
            if (DateTimeOffset.Now - lastPoolUpdateTime >= TimeSpan.FromSeconds(5))
            {
                foreach (var selector in poolSelectors.Values)
                    await selector.Update();

                lastPoolUpdateTime = DateTimeOffset.Now;
            }

            if (DateTimeOffset.Now - lastPoolRefreshTime >= TimeSpan.FromHours(1))
            {
                foreach (var selector in poolSelectors.Values)
                    await selector.Update();

                poolSelectors.Clear();

                lastPoolRefreshTime = DateTimeOffset.Now;
            }
        }

        private async Task processBundle(MatchmakingQueueUpdateBundle bundle)
        {
            foreach (var user in bundle.DeclinedUsers)
            {
                // Right now this will just delay the user from being included in matchmaking for a set period.
                // This will be silent to users affected (see `MatchmakingQueue.matchUsers`).
                //
                // TODO: we should probably let the players know that they have been penalised.
                memoryCache.Set(queue_ban_start_time(user.UserId), bundle.Queue.Clock.UtcNow);
            }

            foreach (var user in bundle.RemovedUsers)
                await hub.Clients.Client(user.Identifier).SendAsync(nameof(IMatchmakingClient.MatchmakingQueueLeft));

            foreach (var user in bundle.AddedUsers)
            {
                await hub.Clients.Client(user.Identifier).SendAsync(nameof(IMatchmakingClient.MatchmakingQueueJoined));
                await hub.Clients.Client(user.Identifier).SendAsync(nameof(IMatchmakingClient.MatchmakingQueueStatusChanged), new MatchmakingQueueStatus.Searching());
            }

            foreach (var group in bundle.RecycledGroups)
            {
                DogStatsd.Increment($"{statsd_prefix}.groups.recycled");

                foreach (var user in group.Users)
                    await hub.Groups.RemoveFromGroupAsync(user.Identifier, group.Identifier);
            }

            foreach (var group in bundle.FormedGroups)
            {
                DogStatsd.Increment($"{statsd_prefix}.groups.formed");

                foreach (var user in group.Users)
                    await hub.Groups.AddToGroupAsync(user.Identifier, group.Identifier, CancellationToken.None);

                // Obsolete method call for older clients that support quick play.
                // It is not important that this is invoked for ranked play too, because these clients can only queue for quick play in the first place.
                await hub.Clients.Group(group.Identifier).SendAsync(nameof(IMatchmakingClient.MatchmakingRoomInvited));

                await hub.Clients.Group(group.Identifier).SendAsync(nameof(IMatchmakingClient.MatchmakingRoomInvitedWithParams), new MatchmakingRoomInvitationParams
                {
                    Type = bundle.Queue.Pool.type.ToPoolType()
                });

                await hub.Clients.Group(group.Identifier).SendAsync(nameof(IMatchmakingClient.MatchmakingQueueStatusChanged), new MatchmakingQueueStatus.MatchFound());
            }

            foreach (var group in bundle.CompletedGroups)
            {
                DogStatsd.Increment($"{statsd_prefix}.groups.completed");

                foreach (var user in group.Users)
                    DogStatsd.Timer($"{statsd_prefix}.queue.duration", (DateTimeOffset.Now - user.SearchStartTime).TotalMilliseconds, tags: [$"queue:{bundle.Queue.Pool.DisplayName}"]);

                foreach (double rating in group.DeltaRatings())
                    DogStatsd.Histogram($"{statsd_prefix}.groups.ratingdelta", rating, tags: [$"queue:{bundle.Queue.Pool.DisplayName}"]);

                string roomName;

                switch (bundle.Queue.Pool.type)
                {
                    case matchmaking_pool_type.ranked_play:
                        string userName1;
                        string userName2;

                        using (var db = databaseFactory.GetInstance())
                        {
                            userName1 = (await db.GetUsernameAsync(group.Users[0].UserId))!;
                            userName2 = (await db.GetUsernameAsync(group.Users[1].UserId))!;
                        }

                        roomName = $"{bundle.Queue.Pool.DisplayName}: {userName1} vs {userName2}";
                        break;

                    default:
                        roomName = bundle.Queue.Pool.DisplayName;
                        break;
                }

                string roomPassword = Guid.NewGuid().ToString();

                long roomId = await sharedInterop.CreateRoomAsync(AppSettings.BanchoBotUserId, new MultiplayerRoom(0)
                {
                    Settings =
                    {
                        Name = roomName,
                        MatchType = bundle.Queue.Pool.type.ToMatchType(),
                        Password = roomPassword
                    }
                });

                if (!poolSelectors.TryGetValue(bundle.Queue.Pool.id, out MatchmakingBeatmapSelector? beatmapSelector))
                    poolSelectors[bundle.Queue.Pool.id] = beatmapSelector = await MatchmakingBeatmapSelector.Initialise(bundle.Queue.Pool, databaseFactory);

                // Initialise the room and users
                using (var roomUsage = await rooms.GetForUse(roomId, true))
                {
                    roomUsage.Item = await ServerMultiplayerRoom.InitialiseMatchmakingRoomAsync(roomId, roomController, databaseFactory, eventDispatcher, loggerFactory, bundle.Queue.Pool,
                        group.Users, beatmapSelector, this);
                }

                await hub.Clients.Group(group.Identifier).SendAsync(nameof(IMatchmakingClient.MatchmakingRoomReady), roomId, roomPassword);

                foreach (var user in group.Users)
                    await hub.Groups.RemoveFromGroupAsync(user.Identifier, group.Identifier);

                await eventDispatcher.PostMatchmakingRoomCreatedAsync(roomId, new MatchmakingRoomCreatedEventDetail
                {
                    pool_id = (int)bundle.Queue.Pool.id
                });
            }
        }

        private async Task<MatchmakingQueueUser> createUserAsync(MultiplayerClientState state, matchmaking_pool pool)
        {
            using (var db = databaseFactory.GetInstance())
            {
                matchmaking_user_stats? stats = await db.GetMatchmakingUserStatsAsync(state.UserId, pool.id);

                if (stats == null)
                {
                    // Estimate initial elo from PP.
                    double pp = await db.GetUserPPAsync(state.UserId, pool.ruleset_id, pool.variant_id);
                    double eloEstimate = -4000 + 600 * Math.Log(pp + 4000);

                    await db.UpdateMatchmakingUserStatsAsync(stats = new matchmaking_user_stats
                    {
                        user_id = (uint)state.UserId,
                        pool_id = pool.id,
                        EloData =
                        {
                            InitialRating = new EloRating(eloEstimate),
                            Rating = new EloRating(eloEstimate)
                        }
                    });
                }

                // torii: sin dificultad elegida no se entra. El picker vivia solo en la
                // pantalla de ranked play, asi que el atajo del toolbar lo salteaba: el
                // que entraba por ahi quedaba emparejado SOLO por elo, contra cualquier
                // dificultad. Asi termina alguien de 5.4 jugando contra alguien de 7.5.
                //
                // El cliente nuevo lo pide antes y con una ventana propia; esto es el
                // porton, para que no dependa de que el cliente se porte bien.
                double? comfortPick = await db.GetComfortPickStarRatingAsync(state.UserId, pool.ruleset_id);

                if (comfortPick == null)
                    throw new InvalidStateException("You need to set your Star Rating first.");

                return new MatchmakingQueueUser(state.ConnectionId)
                {
                    UserId = state.UserId,
                    Rating = stats.EloData.Rating,
                    StarRating = comfortPick,
                    QueueBanStartTime = memoryCache.Get<DateTimeOffset?>(queue_ban_start_time(state.UserId)) ?? DateTimeOffset.MinValue
                };
            }
        }
    }
}
