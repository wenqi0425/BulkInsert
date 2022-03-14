using BulkInsert.Models;

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{
    /// <summary>
    /// to insert data into Genres table
    /// </summary>
    public class GenresInsert
    {
        public void InsertGenre(SqlConnection connection, HashSet<string> genres)
        {
            DataTable genreTable = new DataTable("Genres");
            genreTable.Columns.Add("genreId", typeof(int));
            genreTable.Columns.Add("Genre", typeof(string));

            int counter = 0;
            List<String> genresList = new List<String>(genres);

            foreach (string item in genresList)
            {                
                DataRow genreRow = genreTable.NewRow();
                //int index = genresList.IndexOf(item);
                //if (index == -1) Console.WriteLine("the item: "+ item+ " is not found in genres.");
                genreRow["genreId"] = counter;
                counter++;
                genreRow["Genre"] = item;
                genreTable.Rows.Add(genreRow);
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "Genres";
            bulkCopy.WriteToServer(genreTable);
            bulkCopy.BulkCopyTimeout = 0;
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
