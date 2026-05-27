using LiqBot.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LiqBot.Logic;

public class BotLogic
{
    private readonly BotConfig m_config;
    private readonly LiqBot.Logic.IWebSocketObserverFactory m_wsFactory;
    private readonly ILogger<BotLogic> m_logger;

    #region コンストラクタ
    public BotLogic(
        IOptions<BotConfig> inOptions,
        IWebSocketObserverFactory inWsFactory,
        ILogger<BotLogic> inLogger
    )
    {
        this.m_config = inOptions.Value;
        this.m_wsFactory = inWsFactory;
        this.m_logger = inLogger;
    }
    #endregion

    public async Task RunAsync(CancellationToken ct)
    {
        this.m_logger.LogInformation("Starting LiquidationBot...");

        // jsonの絶対パスを構築
        string payloadPath = Path.Combine(
            AppContext.BaseDirectory,
            "Payloads",
            "hyperliquid_eth_l2.json"
        );
        // 購読したいメッセージの準備
        string subscribePayload = "{}";

        // factoryでインスタンス生成
        using var observer = this.m_wsFactory.CreateWebSocketObserver(
            "wss://api.hyperliquid.xyz/ws",
            subscribePayload
        );

        // データ受信時のイベントハンドラを登録
        observer.MessageReceived += (json) =>
        {
            // ここで清算判定ロジックを実行
            this.m_logger.LogInformation(
                $"データ受信: {json.Substring(0, Math.Min(json.Length, 100))}..."
            );
        };

        // 受信開始
        await observer.RunAsync(ct);
    }
}
