using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{
    public class NameAndProfessionsInsert
    {
        public void InsertNameAndProfessions(SqlConnection connection, List<KeyValuePair<string, List<int>>> nameAndProfessionsPair)
        {
            DataTable nameAndProfessionsPairTable = new DataTable("NameAndProfessions");
            nameAndProfessionsPairTable.Columns.Add("nconst", typeof(string));
            nameAndProfessionsPairTable.Columns.Add("professionId", typeof(int));

            foreach (KeyValuePair<String, List<int>> entry in nameAndProfessionsPair)
            {
                var values = entry.Value;  // List<int>

                foreach (var professionId in values)
                {
                    DataRow nameAndProfessionsTableRow = nameAndProfessionsPairTable.NewRow();
                    nameAndProfessionsTableRow["nconst"] = entry.Key;
                    nameAndProfessionsTableRow["professionId"] = professionId;
                    nameAndProfessionsPairTable.Rows.Add(nameAndProfessionsTableRow);
                }
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "NameAndProfessions";
            bulkCopy.WriteToServer(nameAndProfessionsPairTable);
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
