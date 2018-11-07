using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace com.hnlzwang.ImportAwareVideoGPS.Tools
{
    public sealed class NPgSqlHelper
    {
        #region private
        /// <summary>
        /// existing connection string
        /// </summary>
        private string connectionString;
        /// <summary>
        /// Internal function to prepare a command for execution by the database
        /// </summary>
        /// <param name="cmd">Existing command object</param>
        /// <param name="conn">Database connection object</param>
        /// <param name="trans">Optional transaction object</param>
        /// <param name="cmdType">Command type, e.g. stored procedure</param>
        /// <param name="cmdText">Command test</param>
        /// <param name="commandParameters">Parameters for the command</param>
        private void PrepareCommand(NpgsqlCommand cmd,NpgsqlConnection conn,NpgsqlTransaction trans,CommandType cmdType,string cmdText,params NpgsqlParameter[] commandParameters)
        {

            //Open the connection if required
            if(conn.State != ConnectionState.Open)
                conn.Open();

            //Set up the command
            cmd.Connection = conn;
            if (!String.IsNullOrEmpty(cmdText))
            {
                cmd.CommandText = cmdText;
            }

            cmd.CommandType = cmdType;

            //Bind it to the transaction if it exists
            if(trans != null)
                cmd.Transaction = trans;

            // Bind the parameters passed in
            if(commandParameters != null)
            {
                foreach(NpgsqlParameter parm in commandParameters)
                {
                    if(parm.Value == null || parm.Value.ToString() == "")
                        parm.Value = DBNull.Value;
                    cmd.Parameters.Add(parm);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        private string GetQueryPageSQL(string sql,int pageIndex,int pageSize)
        {
            if(pageIndex > -1 && pageSize > 0)
            {
                int start = (pageIndex - 1) * pageSize;
                return String.Format("{0} LIMIT {1} OFFSET {2}",sql,pageSize,start);
            }
            else
            {
                return sql;
            }
        }
        #endregion

        /// <summary>
        /// set connection string
        /// </summary>
        /// connStr = "User ID=postgres;Password=123;Server=localhost;Port=5432;Database=test;";
        /// <param name="connStr">connection string</param>
        public void SetConnectionString(string connStr)
        {
            connectionString = connStr;
        }

        /// <summary>
        /// get data table by the sql string
        /// </summary>
        /// <param name="sql">the sql string</param>
        /// <returns>data table</returns>
        public System.Data.DataTable GetDataTable(string sql)
        {
            //if (SqlInjection.Checked(sql))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                NpgsqlCommand cmd = new NpgsqlCommand();
                try
                {
                    using(NpgsqlConnection conn = new NpgsqlConnection(connectionString))
                    {
                        PrepareCommand(cmd,conn,null,CommandType.Text,sql);
                        using(NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                        {
                            DataTable dt = new DataTable();
                            da.Fill(dt);
                            return dt;
                        }
                    }
                }
                catch(Exception ex)
                {
                    throw new Exception(sql,ex);
                }
            }
        }

        /// <summary>
        /// get data table by the command
        /// </summary>
        /// <param name="cmdType">the CommandType (text)</param>
        /// <param name="cmdText">the sql command</param>
        /// <param name="commandParameters">an array of paramters used to execute the command</param>
        /// <returns>data table</returns>
        public System.Data.DataTable GetDataTable(System.Data.CommandType cmdType,string cmdText,params NpgsqlParameter[] commandParameters)
        {
            //if (SqlInjection.Checked(cmdText))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                NpgsqlCommand cmd = new NpgsqlCommand();
                try
                {
                    using(NpgsqlConnection conn = new NpgsqlConnection(connectionString))
                    {
                        PrepareCommand(cmd,conn,null,cmdType,cmdText,commandParameters);
                        using(NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                        {
                            DataTable dt = new DataTable();
                            da.Fill(dt);
                            return dt;
                        }
                    }
                }
                catch(Exception ex)
                {
                    throw new Exception(cmdText,ex);
                }
            }
        }

        /// <summary>
        /// get total row count
        /// </summary>
        /// <param name="tableName">table name</param>
        /// <param name="where">where</param>
        /// <returns>row count</returns>
        public int GetRowCount(string tableName,string where)
        {
            //if (SqlInjection.Checked(where))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                string sql = String.Format("select count(*) as cnt from {0} where {1}",tableName,where);
                DataTable dt = GetDataTable(sql);
                if(dt.Rows.Count > 0)
                {
                    return Convert.ToInt32(dt.Rows[0]["cnt"]);
                }
                else
                {
                    return 0;
                }
            }
        }

        /// <summary>
        /// get data table pager by the sql string
        /// </summary>
        /// <param name="pageIndex">page index</param>
        /// <param name="pageSize">page size</param>
        /// <param name="tableName">/param>
        /// <param name="columns"></param>
        /// <param name="order"></param>
        /// <param name="where"></param>
        /// <returns>data table</returns>
        public System.Data.DataTable GetDataTablePager(int pageIndex,int pageSize,string tableName,string columns,string order,string where)
        {
            string sql = string.Format("select {0} from {1} where {2} order by {3}",columns,tableName,where,order);
            //if (SqlInjection.Checked(sql))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                return GetDataTable(GetQueryPageSQL(sql,pageIndex,pageSize));
            }
        }

        /// <summary>
        /// Execute a sql command (that returns no resultset) against an existing database connection 
        /// using the provided parameters.
        /// </summary>
        /// <param name="sql">the sql command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public int ExecuteNonQuery(string sql)
        {
            //if (SqlInjection.Checked(sql))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                NpgsqlCommand cmd = new NpgsqlCommand();
                try
                {
                    //Create a connection
                    using(NpgsqlConnection connection = new NpgsqlConnection(connectionString))
                    {

                        //Prepare the command
                        PrepareCommand(cmd,connection,null,CommandType.Text,sql);

                        //Execute the command
                        int val = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        return val;
                    }
                }
                catch(Exception ex)
                {
                    throw new Exception(sql,ex);
                }
            }
        }

        /// <summary>
        /// Execute a sql command (that returns no resultset) against an existing database connection 
        /// using the provided parameters.
        /// </summary>
        /// <param name="cmdType">the command type (stored procedure, text, etc.)</param>
        /// <param name="cmdText">the stored procedure name or sql command</param>
        /// <param name="commandParameters">an array of paramters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public int ExecuteNonQuery(System.Data.CommandType cmdType,string cmdText,params NpgsqlParameter[] commandParameters)
        {
            //if (SqlInjection.Checked(cmdText))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                NpgsqlCommand cmd = new NpgsqlCommand();
                try
                {
                    //Create a connection
                    using(NpgsqlConnection connection = new NpgsqlConnection(connectionString))
                    {

                        //Prepare the command
                        PrepareCommand(cmd,connection,null,cmdType,cmdText,commandParameters);
                        //Execute the command
                        int val = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        return val;
                    }
                }
                catch(Exception ex)
                {
                    throw new Exception(cmdText,ex);
                }
            }
        }

        /// <summary>
        /// Execute a sql command (that returns no resultset) against an existing database connection 
        /// using the provided parameters.
        /// </summary>
        /// <param name="cmdType">the command type (stored procedure, text, etc.)</param>
        /// <param name="cmdText">the stored procedure name or sql command</param>
        /// <param name="commandParameters">an array of paramters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public object[] ExecuteStoredProcedure(System.Data.CommandType cmdType,string cmdText,int cmdTimeOut,params NpgsqlParameter[] commandParameters)
        {
            //if (SqlInjection.Checked(cmdText))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                NpgsqlCommand cmd = new NpgsqlCommand();

                try
                {
                    //Create a connection
                    using(NpgsqlConnection connection = new NpgsqlConnection(connectionString))
                    {

                        //Prepare the command
                        PrepareCommand(cmd,connection,null,cmdType,cmdText,commandParameters);
                        cmd.CommandTimeout = cmdTimeOut;

                        var dr = cmd.ExecuteReader();
                        dr.Read();
                        object[] result = new object[dr.FieldCount];
                        for(int i = 0;i < dr.FieldCount;i++)
                        {
                            result[i] = dr[i];
                        }
                        return result;
                    }
                }
                catch(Exception ex)
                {
                    throw new Exception(cmdText,ex);
                }
            }
        }

        /// <summary>
        /// Execute list sql command (that returns no resultset) against an existing database connection 
        /// </summary>
        /// <param name="listSql">the list sql string</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public int ExecuteNonQuery(List<string> listSql)
        {
            //if (SqlInjection.Checked(listSql.ToString()))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                NpgsqlCommand cmd = new NpgsqlCommand();

                //Create a connection
                NpgsqlConnection conn = new NpgsqlConnection(connectionString);
                conn.Open();
                NpgsqlTransaction trans = conn.BeginTransaction();
                PrepareCommand(cmd,conn,trans,CommandType.Text,null,null);
                try
                {
                    int count = 0;
                    for(int n = 0;n < listSql.Count;n++)
                    {
                        string strSql = listSql[n];
                        if(strSql.Trim().Length > 1)
                        {
                            cmd.CommandText = strSql;
                            count += cmd.ExecuteNonQuery();
                        }
                    }
                    trans.Commit();
                    cmd.Parameters.Clear();
                    return count;
                }
                catch(Exception ex)
                {
                    trans.Rollback();
                    cmd.Parameters.Clear();
                    throw new Exception(listSql.ToString(),ex);
                }
                finally
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        /// Execute list sql command (that returns no resultset) against an existing database connection 
        /// </summary>
        /// <param name="listCmdType">the list command type (stored procedure, text, etc.)</param>
        /// <param name="listCmdType">the list stored procedure name or sql command</param>
        /// <param name="listCmdType">the list array of paramters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public int ExecuteNonQuery(List<CommandType> listCmdType,List<string> listCmdText,List<NpgsqlParameter[]> listCmdParameters)
        {
            //if (SqlInjection.Checked(listCmdText.ToString()))
            //{
            //    throw new Exception("The sql command include sqlInjection!");
            //}
            //else
            {
                NpgsqlCommand cmd = new NpgsqlCommand();

                //Create a connection
                NpgsqlConnection conn = new NpgsqlConnection(connectionString);
                conn.Open();
                NpgsqlTransaction trans = conn.BeginTransaction();
                try
                {
                    int count = 0;
                    for(int n = 0;n < listCmdText.Count;n++)
                    {
                        cmd.Parameters.Clear();
                        PrepareCommand(cmd,conn,trans,listCmdType[n],listCmdText[n],listCmdParameters[n]);
                        count += cmd.ExecuteNonQuery();
                    }
                    trans.Commit();
                    cmd.Parameters.Clear();
                    return count;
                }
                catch(Exception ex)
                {
                    trans.Rollback();
                    cmd.Parameters.Clear();
                    throw new Exception(listCmdText.ToString(),ex);
                }
                finally
                {
                    conn.Close();
                }
            }
        }

    }
}
