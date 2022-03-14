using BulkInsert.Models;

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{
    class SqlSelector
    {
        public List<TitleBasic> SelectAllTitles(SqlConnection connection)
        {
            List<TitleBasic> allTitles = new List<TitleBasic>();

            SqlCommand command = new SqlCommand("SELECT TOP (1000) [Tconst],[titleType],[PrimaryTitle],[OriginalTitle] " +
                                                ",[IsAdult],[StartYear],[EndYear],[runtimeMinutes]" +
                                                "FROM [RawDB].[dbo].[TitleBasics] WHERE IsAdult = 0", connection);
            SqlDataReader sqlReader = command.ExecuteReader();
            while (sqlReader.Read())
            {
                allTitles.Add(new TitleBasic(sqlReader));
            }
            sqlReader.Close();

            return allTitles;
        }
    }
}
