using System.Net.WebSockets;
using System.Text;
using Microsoft.Extensions.Logging;

namespace LiqBot.Logic;

#region websocketファクトリインターフェース 
public interface IWebSocketObserverFactory
{
    WebSocketObserver CreateWebSocketObserver(string inWssUrl, string inJsonPayload);
}
#endregion


public class WebSocketObserverFactory : IWebSocketObserverFactory
{
    private readonly ILoggerFactory m_loggerFactory;

    public WebSocketObserverFactory(ILoggerFactory inLoggerFactory)
    {
        this.m_loggerFactory = inLoggerFactory;
    }

    // factoryメソッド、外部からのWebSocketObserverクラスインスタンス生成はこのメソッドで行う
    public WebSocketObserver CreateWebSocketObserver(string inWssUrl, string inJsonPayload)
    {
        var logger = this.m_loggerFactory.CreateLogger<WebSocketObserver>();
        return new WebSocketObserver(inWssUrl, inJsonPayload, logger);
    }
}

public class WebSocketObserver : IDisposable
{
    // メンバ変数
    private ClientWebSocket m_webSocket;
    private readonly ILogger<WebSocketObserver> m_logger;
    private string m_wssUrl;
    private string m_jsonPayload;

    // 外部からサブスクライブされるイベント
    public event Action<string>? MessageReceived;
    public event Action? Disconnected;

    // コンストラクタ、internalとしてファクトリメソッド以外での生成を禁止する
    internal WebSocketObserver(
        string inWssUrl,
        string inJsonPayload,
        ILogger<WebSocketObserver> logger
    )
    {
        this.m_wssUrl = inWssUrl;
        this.m_jsonPayload = inJsonPayload;
        this.m_webSocket = new ClientWebSocket();
        this.m_logger = logger;
    }

    // runメソッド
    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await this.ConnectAsync(ct);

                await this.SubscribeAsync(ct);

                await this.StartReceivingLoopAsync(ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                this.m_logger.LogWarning($"Disconnected. ReConnecting 3sec later");
                // 切断されたことを外部に伝える
                Disconnected?.Invoke();

                try
                {
                    await Task.Delay(3000, ct);

                }
                catch (OperationCanceledException)
                {
                    // タスクのキャンセルが要求された場合はループを抜ける
                    break;
                }
            }
        }
    }

    // 接続メソッド
    private async Task ConnectAsync(CancellationToken ct)
    {
        // 接続試行
        if (this.m_webSocket.State != WebSocketState.Open)
        {
            // 古いインスタンスがあれば作り直し
            this.m_webSocket.Dispose();
            this.m_webSocket = new ClientWebSocket();

            this.m_logger.LogInformation($"WebSocket Connected: {this.m_wssUrl}");
            // 接続待機
            await this.m_webSocket.ConnectAsync(new Uri(this.m_wssUrl), ct);
            m_logger.LogInformation("Successfully Connected!");
        }
        else
        {
            this.m_logger.LogInformation("WebSocket Closed.");
            Disconnected?.Invoke();
        }
    }

    // サブスクライブメソッド
    private async Task SubscribeAsync(CancellationToken ct)
    {
        if (this.m_webSocket.State == WebSocketState.Open)
        {
            var bytes = Encoding.UTF8.GetBytes(this.m_jsonPayload);
            await this.m_webSocket.SendAsync(
                new ArraySegment<byte>(bytes),
                WebSocketMessageType.Text,
                true,
                ct
            );
            this.m_logger.LogDebug($"Sending message: {this.m_jsonPayload}");
        }
        else
        {
            this.m_logger.LogInformation("WebSocket Closed.");
            Disconnected?.Invoke();
        }
    }

    // 接続メソッド
    private async Task StartReceivingLoopAsync(CancellationToken ct)
    {
        // 接続試行
        if (this.m_webSocket.State == WebSocketState.Open)
        {
            this.m_logger.LogInformation("Recieving Events...");

            // 接続成功したら受信ループ開始
            await ReceiveLoopAsync(ct);
        }
        else
        {
            this.m_logger.LogInformation("WebSocket Closed.");
            Disconnected?.Invoke();

        }
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        var buffer = new byte[8192];

        while (this.m_webSocket.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            using var ms = new MemoryStream();
            WebSocketReceiveResult result;

            do
            {
                result = await this.m_webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
                ms.Write(buffer, 0, result.Count);
            } while (!result.EndOfMessage);

            // 戻り値がクローズなら脱出して終了
            if (result.MessageType == WebSocketMessageType.Close)
            {
                await this.m_webSocket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Server requested close",
                    ct
                );
                break;
            }

            // 受信データを文字列に
            string message = Encoding.UTF8.GetString(ms.ToArray());

            // 外部クラスへデータを渡す
            MessageReceived?.Invoke(message);
        }
    }

    public void Dispose()
    {
        if (this.m_webSocket != null)
        {
            this.m_webSocket.Dispose();
        }
    }
}
