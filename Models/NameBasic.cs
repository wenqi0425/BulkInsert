using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert.Models
{
    public class NameBasic
    {
        public string nconst { get; set; }
        public string primaryName { get; set; }
        public int? birthYear { get; set; }
        public int? deathYear { get; set; }
        public string primaryProfession { get; set; }
        //public string knownForTitles { get; set; }

        public NameBasic()
        {

        }

        public NameBasic(string[] splitLine)
        {
            nconst = splitLine[0];
            primaryName = splitLine[1];
            birthYear = CheckIntForNull(splitLine[2]);
            deathYear = CheckIntForNull(splitLine[3]);
            primaryProfession = splitLine[4];
            //knownForTitles = splitLine[5];
        }

        private static int? CheckIntForNull(string input)
        {
            if (input == "\\N")
            {
                return null;
            }
            return int.Parse(input);
        }

        public NameBasic(SqlDataReader sqlReader)
        {
            nconst = sqlReader["nconst"].ToString();
            primaryName = sqlReader["primaryName"].ToString();
            
            if (sqlReader.IsDBNull(sqlReader.GetOrdinal("birthYear")))
            {
                birthYear = null;
            }
            else
            {
                birthYear = int.Parse(sqlReader["birthYear"].ToString());
            }

            if (sqlReader.IsDBNull(sqlReader.GetOrdinal("deathYear")))
            {
                deathYear = null;
            }
            else
            {
                deathYear = int.Parse(sqlReader["deathYear"].ToString());
            }

            primaryProfession = sqlReader["primaryProfession"].ToString();
            //knownForTitles = sqlReader["knownForTitles"].ToString();
        }
    }
}
