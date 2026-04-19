using LiqBot.Config;
using LiqBot.Logic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// ビルダー作成
var builder = Host.CreateApplicationBuilder(args);

// factory登録
builder.Services.AddSingleton<IWebSocketObserverFactory, WebSocketObserverFactory>();

// BotConfigの紐付け
builder.Services.Configure<BotConfig>(builder.Configuration.GetSection("BotConfig"));

// Botロジッククラス登録
builder.Services.AddSingleton<BotLogic>();

// ホストのビルド
using IHost host = builder.Build();

// Run
var bot = host.Services.GetRequiredService<BotLogic>();

// Ctrl + c で停止するためにCancellationTokenを用意
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (s, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

await bot.RunAsync(cts.Token);
