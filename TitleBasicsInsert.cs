using BulkInsert.Models;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{
    /// <summary>
    /// to insert data into TitleBasics table
    /// </summary>
    public class TitleBasicsInsert : IInsert
    {
        public void InsertTitleBasic(SqlConnection connection, List<TitleBasic> allTitles)
        {
            DataTable titleTable = new DataTable("TitleBasics");
            titleTable.Columns.Add("tconst", typeof(string));
            titleTable.Columns.Add("titleType", typeof(string));
            titleTable.Columns.Add("primaryTitle", typeof(string));
            titleTable.Columns.Add("originalTitle", typeof(string));
            titleTable.Columns.Add("isAdult", typeof(bool));
            titleTable.Columns.Add("startYear", typeof(int));
            titleTable.Columns.Add("endYear", typeof(int));
            titleTable.Columns.Add("runtimeMinutes", typeof(int));

            foreach (TitleBasic title in allTitles)
            {
                DataRow titleRow = titleTable.NewRow();
                titleRow["tconst"] = title.tconst;
                titleRow["titleType"] = title.titleType;
                titleRow["primaryTitle"] = title.primaryTitle;
                titleRow["originalTitle"] = title.originalTitle;
                titleRow["isAdult"] = title.isAdult;
                AddValueToRow(title.startYear, titleRow, "startYear");
                AddValueToRow(title.endYear, titleRow, "endYear");
                AddValueToRow(title.runtimeMinutes, titleRow, "runtimeMinutes");
                titleTable.Rows.Add(titleRow);
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "TitleBasics";

            bulkCopy.WriteToServer(titleTable);
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
