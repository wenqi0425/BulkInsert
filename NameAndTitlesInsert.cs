using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BulkInsert.Models;

namespace BulkInsert
{
    public class NameAndTitlesInsert
    {
        public void InsertNameAndTitles(SqlConnection connection, List<KeyValuePair<string, string[]>> nameAndTitlesPair)
        {
            DataTable nameAndTitlesPairTable = new DataTable("NameAndTitles");
            nameAndTitlesPairTable.Columns.Add("nconst", typeof(string));
            nameAndTitlesPairTable.Columns.Add("tconst", typeof(string));

            foreach (KeyValuePair<String, string[]> entry in nameAndTitlesPair)
            {
                var values = entry.Value;  

                foreach (var tconst in values)
                {
                    DataRow nameAndTitlesTableRow = nameAndTitlesPairTable.NewRow();
                    nameAndTitlesTableRow["nconst"] = entry.Key;
                    nameAndTitlesTableRow["tconst"] = tconst;
                    nameAndTitlesPairTable.Rows.Add(nameAndTitlesTableRow);
                }
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "NameAndTitles";
            bulkCopy.WriteToServer(nameAndTitlesPairTable);
            //bulkCopy.BulkCopyTimeout = 0;
        }
    }
}
