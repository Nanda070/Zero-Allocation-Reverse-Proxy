using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace ZeroAllocationReverseProxy;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var listenEndpoint = new IPEndPoint(IPAddress.Loopback, 8080);
        var upstreamEndpoint = new IPEndPoint(IPAddress.Loopback, 5000);

        using var listener = new Socket(listenEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        listener.Bind(listenEndpoint);
        listener.Listen(1024);

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        while (!cts.IsCancellationRequested)
        {
            Socket client;
            try
            {
                client = await listener.AcceptAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            _ = AcceptAndRelayAsync(client, upstreamEndpoint, cts.Token);
        }
    }

    private static async Task AcceptAndRelayAsync(Socket client, EndPoint upstreamEndpoint, CancellationToken cancellationToken)
    {
        try
        {
            using var upstream = new Socket(upstreamEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            await upstream.ConnectAsync(upstreamEndpoint, cancellationToken).ConfigureAwait(false);
            await PipelineConnection.RelayAsync(client, upstream, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            client.Dispose();
        }
    }
}
