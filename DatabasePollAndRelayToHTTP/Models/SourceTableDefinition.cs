//Kremowka End User Licence and Service Agreement. Version 1.0 (EULSA v1.0).
namespace DatabasePollAndRelayToHTTP.Models
{
    public class SourceTableDefinition
    {
        public long RowVersion { get; set; }
        public string Name { get; set; }
        public string RowVersionColumnName { get; set; }
        public string CommandText { get; set; }
        public int CommandType { get; set; }
        public int SourceTableDefinitionId { get; set; }
        public int Limit { get; set; }
        public int TablePollInterval { get; set; }
        public string Endpoint { get; set; }
        public bool Stopping { get; set; }
    }
}