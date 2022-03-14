using BulkInsert.Models;
using System.Collections.Generic;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;



namespace BulkInsert
{
    interface IInsert
    {
        void InsertTitleBasic(SqlConnection connection, List<TitleBasic> allTitles);       
    }
}
