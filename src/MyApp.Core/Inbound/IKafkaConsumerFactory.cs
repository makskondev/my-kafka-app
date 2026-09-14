using Confluent.Kafka;

namespace MyApp.Core.Inbound;

/// <summary>
/// Абстракция создания Kafka consumer'а. Вынесена отдельно от InboundConsumerWorker,
/// чтобы в юнит-тестах можно было подставить фейковый IConsumer&lt;string,string&gt;
/// вместо реального подключения к брокеру.
/// </summary>
public interface IKafkaConsumerFactory
{
    IConsumer<string, string> Create(ConsumerConfig config);
}

public sealed class KafkaConsumerFactory : IKafkaConsumerFactory
{
    public IConsumer<string, string> Create(ConsumerConfig config)
        => new ConsumerBuilder<string, string>(config).Build();
}
