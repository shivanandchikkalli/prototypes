using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

var status = new ConcurrentDictionary<int, (int, int)>();

// Simulate a simple Redlock-style distributed lock using 5 Redis instances
// Multiple concurrent "users" will attempt to acquire the lock, do some work, and then release it.

const int NumberOfInstances = 5;
var redisPorts = new[] { 6379, 6380, 6381, 6382, 6383 };

var endpoints = redisPorts.Select(p => $"localhost:{p}").ToArray();

// Connect to all Redis instances
var multiplexers = new List<ConnectionMultiplexer>();
foreach (var endpoint in endpoints)
{
    Console.WriteLine($"Connecting to {endpoint}...");
    var mux = await ConnectionMultiplexer.ConnectAsync(endpoint);
    multiplexers.Add(mux);
}

var lockManager = new RedLockManager(multiplexers.Select(m => m.GetDatabase()).ToList());

const int concurrentUsers = 10;
var tasks = new List<Task>();

for (int i = 0; i < concurrentUsers; i++)
{
    var userId = i + 1;
    tasks.Add(Task.Run(async () =>
    {
        int success = 0;
        int failure = 0;

        var rnd = new Random(Guid.NewGuid().GetHashCode());
        // Each user will try multiple times
        for (int attempt = 0; attempt < 10; attempt++)
        {
            var lockKey = "distributed:resource:lock";
            var ttl = TimeSpan.FromSeconds(8);
            Console.WriteLine($"User {userId} attempting to acquire lock (attempt {attempt + 1})...");
            var token = Guid.NewGuid().ToString("N");
            var acquired = await lockManager.AcquireAsync(lockKey, token, ttl, retryCount: 3, retryDelay: TimeSpan.FromMilliseconds(200));
            if (acquired)
            {
                success++;
                Console.WriteLine($"User {userId} ACQUIRED lock. Doing work...");
                // Simulate some work while holding the lock
                await Task.Delay(rnd.Next(500, 2000));
                var released = await lockManager.ReleaseAsync(lockKey, token);
                Console.WriteLine($"User {userId} RELEASED lock: {released}");
                break;
            }
            else
            {
                failure++;
                Console.WriteLine($"User {userId} failed to acquire lock. Retrying later...");
                await Task.Delay(rnd.Next(200, 800));
            }
        }
        status.AddOrUpdate(userId, (success, failure), (x, y) => {
            return (0, 0);
        });
    }));
}

await Task.WhenAll(tasks);

Console.WriteLine("All users completed. Cleaning up connections...");
foreach (var m in multiplexers)
    m.Dispose();

foreach (var item in status)
{
    Console.WriteLine($"{item.Key} Success = {item.Value.Item1} Failure = {item.Value.Item2}");
}

// --- RedLock manager implementation ---

class RedLockManager
{
    private readonly List<IDatabase> _databases;
    private readonly int _quorum;

    public RedLockManager(List<IDatabase> databases)
    {
        _databases = databases;
        _quorum = databases.Count / 2 + 1; // majority
    }

    // Try to acquire lock across instances. Returns true if lock acquired.
    public async Task<bool> AcquireAsync(string key, string value, TimeSpan ttl, int retryCount = 3, TimeSpan? retryDelay = null)
    {
        var delay = retryDelay ?? TimeSpan.FromMilliseconds(200);
        var rnd = new Random();

        for (int attempt = 0; attempt < retryCount; attempt++)
        {
            var start = Stopwatch.GetTimestamp();
            int successCount = 0;

            var tasks = _databases.Select(db => db.StringSetAsync(key, value, ttl, when: When.NotExists)).ToArray();
            await Task.WhenAll(tasks);
            successCount = tasks.Count(t => t.Result);

            var elapsedMs = (Stopwatch.GetTimestamp() - start) * 1000 / (double)Stopwatch.Frequency;
            var validity = ttl.TotalMilliseconds - elapsedMs;

            if (successCount >= _quorum && validity > 0)
            {
                return true;
            }

            // Failed to acquire, clean up any partial locks we created
            var releaseTasks = _databases.Select(db => ReleaseIfMatchAsync(db, key, value)).ToArray();
            await Task.WhenAll(releaseTasks);

            // Wait a bit before retrying with jitter
            var jitter = rnd.Next(0, 100);
            await Task.Delay(delay + TimeSpan.FromMilliseconds(jitter));
        }

        return false;
    }

    // Release lock across instances. Returns true if at least one instance deleted the key.
    public async Task<bool> ReleaseAsync(string key, string value)
    {
        var tasks = _databases.Select(db => ReleaseIfMatchAsync(db, key, value)).ToArray();
        await Task.WhenAll(tasks);
        return tasks.Any(t => t.Result);
    }

    // Use a Lua script to delete the key only if its value matches
    private async Task<bool> ReleaseIfMatchAsync(IDatabase db, string key, string value)
    {
        const string script = @"if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        try
        {
            var result = await db.ScriptEvaluateAsync(script, new RedisKey[] { key }, new RedisValue[] { value });
            return (int)result! > 0;
        }
        catch
        {
            return false;
        }
    }
}
