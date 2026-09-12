namespace MyApp.Contracts;

/// <summary>
/// Необязательная самодокументация: связывает класс-обработчик/источник с Code из конфига.
/// В связывании (DI/резолвинге) не участвует - служит визуальной подсказкой в коде
/// и заделом под будущую автоматическую проверку "конфиг ⇄ код" в CI.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class EventTypeAttribute(string code) : Attribute
{
    public string Code { get; } = code;
}
