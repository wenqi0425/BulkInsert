using BulkInsert.Models;

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{
    interface IInsert
    {
        void InsertTitleBasic(SqlConnection connection, System.Collections.Generic.List<TitleBasic> allTitles);
        

    }
}
