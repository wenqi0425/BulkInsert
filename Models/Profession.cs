using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert.Models
{
    public class Profession
    {
        public int professionId { get; set; }
        public string profession { get; set; }

        public Profession()
        {
        }

        public Profession(SqlDataReader sqlReader)
        {
            profession = sqlReader["profession"].ToString();
        }
    }
}
