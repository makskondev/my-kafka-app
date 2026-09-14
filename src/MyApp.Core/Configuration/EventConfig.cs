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

    /// <summary>
    /// Настройки безопасности (SASL_SSL и т.п.), общие для продюсера и всех консьюмеров.
    /// Если null - подключение без аутентификации/шифрования (например, локальный dev-брокер).
    /// </summary>
    public KafkaSecurityOptions? Security { get; init; }
}

public sealed class KafkaSecurityOptions
{
    /// <summary>Значение Confluent.Kafka.SecurityProtocol как строка: "SaslSsl", "Ssl", "SaslPlaintext" и т.д.</summary>
    public required string SecurityProtocol { get; init; }

    /// <summary>Значение Confluent.Kafka.SaslMechanism как строка: "Plain", "ScramSha256", "ScramSha512" и т.д.</summary>
    public required string SaslMechanism { get; init; }

    public string? SaslUsername { get; init; }

    /// <summary>
    /// Не хранить в appsettings.json - задавать через dotnet user-secrets (dev) или
    /// переменные окружения / секрет-хранилище оркестратора (прод). См. README.
    /// </summary>
    public string? SaslPassword { get; init; }

    /// <summary>Путь к CA-сертификату (PEM), которым подписан сертификат брокера.</summary>
    public string? SslCaLocation { get; init; }

    /// <summary>Путь к клиентскому сертификату (PEM) - нужен только при mTLS.</summary>
    public string? SslCertificateLocation { get; init; }

    /// <summary>Путь к приватному ключу клиента (PEM) - нужен только при mTLS.</summary>
    public string? SslKeyLocation { get; init; }

    /// <summary>Пароль от приватного ключа, если он зашифрован. Тоже через секреты, не в appsettings.json.</summary>
    public string? SslKeyPassword { get; init; }

    public bool EnableSslCertificateVerification { get; init; } = true;
}
