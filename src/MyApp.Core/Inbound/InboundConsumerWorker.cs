using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MyApp.Contracts.Inbound;
using MyApp.Core.Configuration;
using MyApp.Core.Kafka;

namespace MyApp.Core.Inbound;

/// <summary>
/// Единый generic-воркер на ВСЕ inbound-события. Новый экземпляр создаётся
/// на каждый Code при регистрации (см. AddInboundEvents) - разработчик обработчика
/// этот класс не пишет и не видит.
/// </summary>
public sealed class InboundConsumerWorker(
    InboundEventRegistration registration,
    IServiceScopeFactory scopeFactory,
    IOptions<KafkaOptions> kafkaOptions,
    IKafkaConsumerFactory consumerFactory,
    ILogger<InboundConsumerWorker> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        // Consume() в Confluent.Kafka блокирующий - выносим цикл в отдельный поток,
        // чтобы не занимать поток пула хоста синхронным ожиданием.
        => Task.Run(() => RunLoop(stoppingToken), stoppingToken);

    private async Task RunLoop(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers,
            GroupId = registration.ConsumerGroup,
            EnableAutoCommit = false, // коммитим только после успешной записи в Oracle
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        config.ApplySecurity(kafkaOptions.Value.Security);

        using var consumer = consumerFactory.Create(config);
        consumer.Subscribe(registration.SourceQueue);

        logger.LogInformation("Inbound worker started. Code={Code}, Queue={Queue}, Group={Group}",
            registration.Code, registration.SourceQueue, registration.ConsumerGroup);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result = null;
            try
            {
                result = consumer.Consume(stoppingToken);
                if (result is null || result.IsPartitionEOF)
                {
                    continue;
                }

                await ProcessMessageAsync(consumer, result, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Inbound processing failed. Code={Code}, Offset={Offset}",
                    registration.Code, result?.TopicPartitionOffset);

                // без DLQ: не коммитим оффсет, следующий Consume() вернёт то же сообщение снова
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        consumer.Close();
        logger.LogInformation("Inbound worker stopped. Code={Code}", registration.Code);
    }

    /// <summary>
    /// Обработка одного сообщения: резолвит обработчик по Code, вызывает его и коммитит offset
    /// только при успехе. Исключение НЕ перехватывается здесь намеренно - его перехватывает
    /// и логирует RunLoop, чтобы offset не закоммитился. Выделено в internal метод для юнит-тестов.
    /// </summary>
    internal async Task ProcessMessageAsync(
        IConsumer<string, string> consumer, ConsumeResult<string, string> result, CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var handler = scope.ServiceProvider
            .GetRequiredKeyedService<IInboundEventHandlerBase>(registration.Code);

        var context = new InboundEventContext(registration.Code, result.Topic, result.Message.Key);
        await handler.HandleAsync(result.Message.Value, context, ct);

        consumer.Commit(result); // обработчик идемпотентен -> повторная доставка безопасна
    }
}
