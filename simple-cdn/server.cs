// <Project Sdk="Microsoft.NET.Sdk.Web">

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using SimpleCdn.Models;

var builder = WebApplication.CreateBuilder(args);

// Listen on port 45000
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(45000);
});

var app = builder.Build();

// Cache TTL in seconds (3600 = 1 hour, 86400 = 1 day)
const int CACHE_TTL_SECONDS = 3600;

// Load CDN origin mappings from file
Dictionary<string, string> cdnOriginMappings = LoadCdnOriginMappings("cdnorigins.json");

// Cache directory
string cacheDir = Path.Combine(Directory.GetCurrentDirectory(), "cache");
if (!Directory.Exists(cacheDir))
{
    Directory.CreateDirectory(cacheDir);
}

// Base path
app.Map("/", () => Results.Ok("Hello from base path"));

// Catch-all for any path + any HTTP method
app.Map("/{**any}", (HttpContext context) =>
{
    return HandleCachedRequest(context, cdnOriginMappings, cacheDir, CACHE_TTL_SECONDS).Result;
});

app.Run();

// Load CDN origin mappings from a JSON file
static Dictionary<string, string> LoadCdnOriginMappings(string filePath)
{
    var mappings = new Dictionary<string, string>();
    
    if (!File.Exists(filePath))
    {
        Console.WriteLine($"Warning: {filePath} not found. Creating empty mappings.");
        return mappings;
    }

    try
    {
        string json = File.ReadAllText(filePath);
        mappings = JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? new Dictionary<string, string>();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error loading CDN mappings: {ex.Message}");
    }

    return mappings;
}

// Hash a string using SHA256
static string HashString(string input)
{
    using (var sha256 = SHA256.Create())
    {
        byte[] hashedBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(hashedBytes).ToLower();
    }
}

// Handle cached request
static async Task<IResult> HandleCachedRequest(
    HttpContext context, 
    Dictionary<string, string> cdnOriginMappings, 
    string cacheDir,
    int cacheTtlSeconds)
{
    string requestUrl = context.Request.Scheme + "://" + context.Request.Host + context.Request.Path + context.Request.QueryString;
    string domain = context.Request.Host.Host;
    string path = context.Request.Path.Value ?? "/";

    Console.WriteLine($"Received request: {requestUrl}");

    string domainHash = HashString(domain);
    string pathHash = HashString(path);

    string domainFolder = Path.Combine(cacheDir, domainHash);
    string cacheFile = Path.Combine(domainFolder, pathHash);

    // Check if cached response exists and is still valid
    if (Directory.Exists(domainFolder) && File.Exists(cacheFile))
    {
        try
        {
            string jsonContent = File.ReadAllText(cacheFile);
            CacheEntry entry = JsonSerializer.Deserialize<CacheEntry>(jsonContent);
            
            if (entry != null)
            {
                long currentTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                long age = currentTimestamp - entry.Timestamp;

                if (age < cacheTtlSeconds)
                {
                    Console.WriteLine($"Cache HIT for {path} (age: {age}s, TTL: {cacheTtlSeconds}s)");
                    return Results.Ok(entry.Content);
                }
                else
                {
                    Console.WriteLine($"Cache EXPIRED for {path} (age: {age}s, TTL: {cacheTtlSeconds}s)");
                    try
                    {
                        File.Delete(cacheFile);
                        Console.WriteLine($"Deleted expired cache file: {cacheFile}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error deleting expired cache file: {ex.Message}");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading cache: {ex.Message}");
        }
    }

    // Cache miss - fetch from origin
    if (!cdnOriginMappings.ContainsKey(domain))
    {
        return Results.BadRequest(new { Error = "Domain not found in CDN mappings", Domain = domain });
    }

    string originUrl = cdnOriginMappings[domain];
    Uri originUri = new Uri(originUrl);
    
    // Construct new URL with origin's scheme and domain but keep path and query from request
    string newUrl = originUri.Scheme + "://" + originUri.Authority + context.Request.Path + context.Request.QueryString;

    try
    {
        using (var client = new HttpClient())
        {
            Console.WriteLine($"Fetching from origin: {newUrl}");

            var response = await client.GetAsync(newUrl);

            string responseContent = await response.Content.ReadAsStringAsync();

            Console.WriteLine($"Fetched from origin: {newUrl} - Status: {response.StatusCode}");

            // Create cache folder if not exists
            if (!Directory.Exists(domainFolder))
            {
                Directory.CreateDirectory(domainFolder);
            }

            // Create cache entry with timestamp
            var cacheEntry = new CacheEntry
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Content = responseContent
            };

            // Write to cache file as JSON
            string cacheJson = JsonSerializer.Serialize(cacheEntry);
            File.WriteAllText(cacheFile, cacheJson);

            Console.WriteLine($"Cache STORED for {path}");

            return Results.Ok(responseContent);
        }
    }
    catch (Exception ex)
    {
        return Results.InternalServerError(new { Error = "Failed to fetch from origin", Details = ex.Message });
    }
}

