namespace MyApp.Core.Configuration;

public sealed class InboundEventConfig
{
    public required string Code { get; init; }
    public required string SourceQueue { get; init; }
    public required string ConsumerGroup { get; init; }

    /// <summary>Полное имя типа-обработчика: "Namespace.ClassName, AssemblyName".</summary>
    public required string Handler { get; init; }
}

public sealed class OutboundEventConfig
{
    public required string Code { get; init; }
    public required string SourceView { get; init; }
    public required string SuccessProcedure { get; init; }
    public required string ErrorProcedure { get; init; }
    public required string TargetTopic { get; init; }
    public int PollingIntervalSeconds { get; init; } = 15;

    /// <summary>Полное имя типа-источника: "Namespace.ClassName, AssemblyName".</summary>
    public required string Source { get; init; }
}

public sealed class KafkaOptions
{
    public required string BootstrapServers { get; init; }
}
