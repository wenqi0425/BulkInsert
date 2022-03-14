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
    public class NameBasicsInsert
    {
        public void InsertName(SqlConnection connection, List<NameBasic> allNames)
        {
            DataTable nameTable = new DataTable("NameBasics");
            nameTable.Columns.Add("nconst", typeof(string));
            nameTable.Columns.Add("primaryName", typeof(string));
            nameTable.Columns.Add("birthYear", typeof(int));
            nameTable.Columns.Add("deathYear", typeof(int));

            foreach (NameBasic name in allNames)
            {
                DataRow nameRow = nameTable.NewRow();
                nameRow["nconst"] = name.nconst;
                nameRow["primaryName"] = name.primaryName;
                AddValueToRow(name.birthYear, nameRow, "birthYear");
                AddValueToRow(name.deathYear, nameRow, "deathYear");
                nameTable.Rows.Add(nameRow);
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "NameBasics";
            bulkCopy.WriteToServer(nameTable);
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
