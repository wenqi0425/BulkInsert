using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{
    public class ProfessionsInsert
    {
        public void InsertProfession(SqlConnection connection, HashSet<string> professions)
        {
            DataTable professionTable = new DataTable("Professions");
            professionTable.Columns.Add("professionId", typeof(int));
            professionTable.Columns.Add("profession", typeof(string));

            int counter = 0;
            List<String> professionsList = new List<String>(professions);

            foreach (string item in professionsList)
            {
                DataRow professionRow = professionTable.NewRow();
                professionRow["professionId"] = counter;
                counter++;
                professionRow["profession"] = item;
                professionTable.Rows.Add(professionRow);
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "Professions";
            bulkCopy.WriteToServer(professionTable);
            //bulkCopy.BulkCopyTimeout = 0;
        }

        public static void AddValueToRow(int? value, DataRow row, string columnName)
        {
            if (value == null)
            {
                row[columnName] = DBNull.Value;
            }
            else
            {
                row[columnName] = value;
            }
        }
    }
}
