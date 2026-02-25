using dolphy_backend.DBManagement;
using dolphy_backend.Implementations;
using dolphy_backend.Interfaces;
using Hangfire;
using Hangfire.PostgreSql;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

//Redis Distributed Cache
builder.Services.AddStackExchangeRedisCache(options =>
{
    options.Configuration = builder.Configuration.GetConnectionString("Redis");
    options.InstanceName = "Dolphy_";
});

// Add services to the container

builder.Services.AddDistributedMemoryCache();

builder.Services.AddScoped<IFileCleaner, FileCleaner>();
builder.Services.AddScoped<IFileHandler, FileHandler>();

builder.Services.AddDbContext<DatabaseHandler>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));
builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
