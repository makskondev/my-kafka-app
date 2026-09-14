using Confluent.Kafka;
using MyApp.Core.Configuration;
using MyApp.Core.Kafka;
using Xunit;

namespace MyApp.Tests.Kafka;

public class KafkaSecurityConfiguratorTests
{
    private static KafkaSecurityOptions ValidOptions() => new()
    {
        SecurityProtocol = "SaslSsl",
        SaslMechanism = "ScramSha512",
        SaslUsername = "myapp-service",
        SaslPassword = "secret",
        SslCaLocation = "/etc/myapp/certs/ca.pem",
        EnableSslCertificateVerification = true,
    };

    [Fact]
    public void ApplySecurity_WhenNull_LeavesConfigUntouched()
    {
        var config = new ConsumerConfig();

        config.ApplySecurity(null);

        Assert.Null(config.SecurityProtocol);
        Assert.Null(config.SaslMechanism);
        Assert.Null(config.SaslUsername);
    }

    [Fact]
    public void ApplySecurity_MapsAllFieldsOntoConsumerConfig()
    {
        var config = new ConsumerConfig();

        config.ApplySecurity(ValidOptions());

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.Equal("myapp-service", config.SaslUsername);
        Assert.Equal("secret", config.SaslPassword);
        Assert.Equal("/etc/myapp/certs/ca.pem", config.SslCaLocation);
        Assert.True(config.EnableSslCertificateVerification);
    }

    [Fact]
    public void ApplySecurity_MapsAllFieldsOntoProducerConfig()
    {
        // ProducerConfig и ConsumerConfig оба наследуются от ClientConfig - проверяем,
        // что один и тот же метод корректно применяется к обоим, без расхождений.
        var config = new ProducerConfig();

        config.ApplySecurity(ValidOptions());

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.Equal("myapp-service", config.SaslUsername);
    }

    [Fact]
    public void ApplySecurity_ThrowsOnUnknownSecurityProtocol()
    {
        var config = new ConsumerConfig();
        var options = new KafkaSecurityOptions
        {
            SecurityProtocol = "NotAProtocol",
            SaslMechanism = "ScramSha512",
        };

        var ex = Assert.Throws<InvalidOperationException>(() => config.ApplySecurity(options));
        Assert.Contains("SecurityProtocol", ex.Message);
        Assert.Contains("NotAProtocol", ex.Message);
    }

    [Fact]
    public void ApplySecurity_ThrowsOnUnknownSaslMechanism()
    {
        var config = new ConsumerConfig();
        var options = new KafkaSecurityOptions
        {
            SecurityProtocol = "SaslSsl",
            SaslMechanism = "NotAMechanism",
        };

        var ex = Assert.Throws<InvalidOperationException>(() => config.ApplySecurity(options));
        Assert.Contains("SaslMechanism", ex.Message);
        Assert.Contains("NotAMechanism", ex.Message);
    }

    [Fact]
    public void ApplySecurity_IsCaseInsensitiveForEnumValues()
    {
        var config = new ConsumerConfig();
        var options = new KafkaSecurityOptions
        {
            SecurityProtocol = "saslssl",
            SaslMechanism = "scramsha512",
        };

        config.ApplySecurity(options);

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
    }
}
