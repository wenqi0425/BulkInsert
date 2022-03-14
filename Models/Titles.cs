using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert.Models
{
    public class Titles
    {
        public string tconst { get; set; }
        //public string titleType { get; set; }
        public string primaryTitle { get; set; }
        public string originalTitle { get; set; }
        public bool isAdult { get; set; }
        public int? startYear { get; set; }
        public int? endYear { get; set; }
        public int? runTime { get; set; }

        public Titles()
        {

        }

        public Titles(string[] splitLine)
        {
            tconst = splitLine[0];
            //titleType = splitLine[1];
            primaryTitle = splitLine[1];
            originalTitle = splitLine[2];
            isAdult = splitLine[3] == "1";
            startYear = CheckIntForNull(splitLine[4]);
            endYear = CheckIntForNull(splitLine[5]);
            runTime = CheckIntForNull(splitLine[6]);
        }

        private static int? CheckIntForNull(string input)
        {
            if (input == "\\N")
            {
                return null;
            }
            return int.Parse(input);
        }

        public Titles(SqlDataReader sqlReader)
        {
            tconst = sqlReader["tconst"].ToString();
            //titleType = sqlReader["titleType"].ToString();
            primaryTitle = sqlReader["primaryTitle"].ToString();
            originalTitle = sqlReader["originalTitle"].ToString();
            isAdult = sqlReader["isAdult"].ToString() == "1";

            if (sqlReader.IsDBNull(sqlReader.GetOrdinal("startYear")))
            {
                startYear = null;
            }
            else
            {
                startYear = int.Parse(sqlReader["startYear"].ToString());
            }

            if (sqlReader.IsDBNull(sqlReader.GetOrdinal("endYear")))
            {
                endYear = null;
            }
            else
            {
                endYear = int.Parse(sqlReader["endYear"].ToString());
            }

            if (sqlReader.IsDBNull(sqlReader.GetOrdinal("runTime")))
            {
                runTime = null;
            }
            else
            {
                runTime = int.Parse(sqlReader["runTime"].ToString());
            }
        }
    }
}
