using Confluent.Kafka;
using MyApp.Core.Configuration;

namespace MyApp.Core.Kafka;

/// <summary>
/// Единая точка применения SASL/SSL-настроек. ConsumerConfig и ProducerConfig
/// оба наследуются от ClientConfig, поэтому один и тот же код конфигурирует
/// и продюсер, и консьюмер - настройки гарантированно не расходятся между ними.
/// </summary>
public static class KafkaSecurityConfigurator
{
    public static void ApplySecurity(this ClientConfig config, KafkaSecurityOptions? security)
    {
        if (security is null)
        {
            return;
        }

        if (!Enum.TryParse<SecurityProtocol>(security.SecurityProtocol, ignoreCase: true, out var protocol))
        {
            throw new InvalidOperationException(
                $"Kafka:Security:SecurityProtocol - unknown value '{security.SecurityProtocol}'. " +
                $"Valid values: {string.Join(", ", Enum.GetNames<SecurityProtocol>())}");
        }

        if (!Enum.TryParse<SaslMechanism>(security.SaslMechanism, ignoreCase: true, out var mechanism))
        {
            throw new InvalidOperationException(
                $"Kafka:Security:SaslMechanism - unknown value '{security.SaslMechanism}'. " +
                $"Valid values: {string.Join(", ", Enum.GetNames<SaslMechanism>())}");
        }

        config.SecurityProtocol = protocol;
        config.SaslMechanism = mechanism;
        config.SaslUsername = security.SaslUsername;
        config.SaslPassword = security.SaslPassword;
        config.SslCaLocation = security.SslCaLocation;
        config.SslCertificateLocation = security.SslCertificateLocation;
        config.SslKeyLocation = security.SslKeyLocation;
        config.SslKeyPassword = security.SslKeyPassword;
        config.EnableSslCertificateVerification = security.EnableSslCertificateVerification;
    }
}
