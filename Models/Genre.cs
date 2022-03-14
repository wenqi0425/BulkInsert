using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert.Models
{
    public class Genre
    {
        public int genreId { get; set; }
        public string genre { get; set; }

        public Genre()
        {
        }

        public Genre(SqlDataReader sqlReader)
        {
            genre = sqlReader["genre"].ToString();
        }
    }
}

