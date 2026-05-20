using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace ZeroAllocationReverseProxy;

internal sealed class PipelineConnection : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly Pipe _receivePipe;
    private readonly Pipe _sendPipe;
    private readonly CancellationToken _cancellationToken;
    private readonly Task _receiveTask;
    private readonly Task _sendTask;
    private bool _disposed;

    public PipelineConnection(Socket socket, CancellationToken cancellationToken)
    {
        _socket = socket ?? throw new ArgumentNullException(nameof(socket));
        _cancellationToken = cancellationToken;

        var pipeOptions = new PipeOptions(
            pool: MemoryPool<byte>.Shared,
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool,
            pauseWriterThreshold: 64 * 1024,
            resumeWriterThreshold: 32 * 1024,
            useSynchronizationContext: false);

        _receivePipe = new Pipe(pipeOptions);
        _sendPipe = new Pipe(pipeOptions);

        _receiveTask = FillReceivePipeAsync();
        _sendTask = FlushSendPipeAsync();
    }

    public PipeReader Input => _receivePipe.Reader;
    public PipeWriter Output => _sendPipe.Writer;

    private async Task FillReceivePipeAsync()
    {
        try
        {
            while (!_cancellationToken.IsCancellationRequested)
            {
                Memory<byte> memory = _receivePipe.Writer.GetMemory(4096);
                int bytesRead = await _socket.ReceiveAsync(memory, SocketFlags.None, _cancellationToken).ConfigureAwait(false);
                if (bytesRead == 0)
                {
                    break;
                }

                _receivePipe.Writer.Advance(bytesRead);
                var flushResult = await _receivePipe.Writer.FlushAsync(_cancellationToken).ConfigureAwait(false);
                if (flushResult.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch
        {
        }
        finally
        {
            await _receivePipe.Writer.CompleteAsync().ConfigureAwait(false);
        }
    }

    private async Task FlushSendPipeAsync()
    {
        try
        {
            while (true)
            {
                var readResult = await _sendPipe.Reader.ReadAsync(_cancellationToken).ConfigureAwait(false);
                var buffer = readResult.Buffer;
                if (!buffer.IsEmpty)
                {
                    await SendSequenceAsync(buffer).ConfigureAwait(false);
                }

                _sendPipe.Reader.AdvanceTo(buffer.End);
                if (readResult.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch
        {
        }
        finally
        {
            await _sendPipe.Reader.CompleteAsync().ConfigureAwait(false);
            try
            {
                _socket.Shutdown(SocketShutdown.Send);
            }
            catch
            {
            }
            _socket.Close();
        }
    }

    private async ValueTask SendSequenceAsync(ReadOnlySequence<byte> buffer)
    {
        foreach (var segment in buffer)
        {
            ReadOnlyMemory<byte> memory = segment;
            while (!memory.IsEmpty)
            {
                int sent = await _socket.SendAsync(memory, SocketFlags.None, _cancellationToken).ConfigureAwait(false);
                if (sent == 0)
                {
                    throw new SocketException((int)SocketError.Shutdown);
                }

                memory = memory.Slice(sent);
            }
        }
    }

    public static async ValueTask RelayAsync(Socket client, Socket upstream, CancellationToken cancellationToken)
    {
        await using var clientConnection = new PipelineConnection(client, cancellationToken);
        await using var upstreamConnection = new PipelineConnection(upstream, cancellationToken);

        var clientToUpstream = RelayPipeAsync(clientConnection.Input, upstreamConnection.Output, cancellationToken);
        var upstreamToClient = RelayPipeAsync(upstreamConnection.Input, clientConnection.Output, cancellationToken);

        await Task.WhenAny(clientToUpstream.AsTask(), upstreamToClient.AsTask()).ConfigureAwait(false);

        await clientConnection.Output.CompleteAsync().ConfigureAwait(false);
        await upstreamConnection.Output.CompleteAsync().ConfigureAwait(false);

        await Task.WhenAll(clientToUpstream.AsTask(), upstreamToClient.AsTask()).ConfigureAwait(false);
    }

    private static async ValueTask RelayPipeAsync(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                var readResult = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = readResult.Buffer;
                if (buffer.IsEmpty && readResult.IsCompleted)
                {
                    break;
                }

                foreach (var segment in buffer)
                {
                    writer.Write(segment.Span);
                }

                var flushResult = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                reader.AdvanceTo(buffer.End);
                if (flushResult.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await writer.CompleteAsync().ConfigureAwait(false);
            await reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _receivePipe.Reader.CancelPendingRead();
        _sendPipe.Reader.CancelPendingRead();

        _receivePipe.Writer.Complete();
        _sendPipe.Writer.Complete();

        _socket.Dispose();

        await Task.WhenAll(_receiveTask, _sendTask).ConfigureAwait(false);
    }
}
