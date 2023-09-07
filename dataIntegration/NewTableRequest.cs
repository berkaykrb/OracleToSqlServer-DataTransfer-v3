using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dataIntegration
{
    internal class NewTableRequest
    {
        public string sourceTableName {get;set;}
        public string targetTableName { get; set; }
        public string[,] columns { get; set; }
        public string sqlQuery { get; set; }
        public bool defaultQuery { get; set; }
    }
}
