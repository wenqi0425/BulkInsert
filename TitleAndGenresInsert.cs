using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{
    public class TitleAndGenresInsert
    {
        public void InsertTitleAndGenre(SqlConnection connection, List<KeyValuePair<string, List<int>>> titleAndGenrePair)
        {
            DataTable titleAndGenreTable = new DataTable("TitlesAndGenres");
            titleAndGenreTable.Columns.Add("tconst", typeof(string));
            titleAndGenreTable.Columns.Add("genreId", typeof(int));

            foreach (KeyValuePair<String, List<int>> entry in titleAndGenrePair)
            {
                var values = entry.Value;  // List<int>

                foreach (var genreId in values)
                {
                    DataRow titleAndGenreTableRow = titleAndGenreTable.NewRow();
                    titleAndGenreTableRow["tconst"] = entry.Key;
                    titleAndGenreTableRow["genreId"] = genreId;
                    titleAndGenreTable.Rows.Add(titleAndGenreTableRow);
                }
            }

            SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "TitlesAndGenres";
            bulkCopy.WriteToServer(titleAndGenreTable);
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
