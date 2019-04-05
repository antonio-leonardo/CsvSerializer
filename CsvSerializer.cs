using System;
using System.Text;
using System.Linq;
using System.Data;
using System.Reflection;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace CsvSerialization
{
    /// <summary>
    /// Abstract class that share all Serializations execution,
    /// only Serialize and Deserialize
    /// </summary>
    /// <typeparam name="TEntity">Maybe POCO class or any object that provides collections</typeparam>
    public abstract class CsvSerializerAbstraction<TEntity>
        where TEntity : class
    {
        /// <summary>
        /// Abstraction of method to Serialize routine
        /// </summary>
        /// <param name="collection"></param>
        /// <returns></returns>
        protected abstract string CustomSerialize(params TEntity[] collection);

        /// <summary>
        /// Abstraction to Deserialize csv string format
        /// </summary>
        /// <param name="csvString"></param>
        /// <returns></returns>
        protected abstract IEnumerable<TEntity> CustomDeserialize(string csvString);
    }

    /// <summary>
    /// Concrete class with DataRow class implementation
    /// </summary>
    public sealed partial class CsvSerializer : CsvSerializerAbstraction<DataRow>
    {
        /// <summary>
        /// This class only be instanciable like a private behavior
        /// </summary>
        private CsvSerializer()
        {
        }

        #region ' Serialize '

        /// <summary>
        /// 
        /// </summary>
        /// <param name="collection"></param>
        /// <returns></returns>
        public static string Serialize(DataTable collection)
        {
            StringBuilder sbColumns = new StringBuilder();

            for (int i = 0; i < collection.Columns.Count; i++)
            {
                if (i == collection.Columns.Count - 1)
                {
                    sbColumns.AppendLine(collection.Columns[i].ColumnName + ";");
                }
                else
                {
                    sbColumns.Append(collection.Columns[i].ColumnName).Append(";");
                }
            }
            return sbColumns.ToString() + (new CsvSerializer()).CustomSerialize(collection.Rows.Cast<DataRow>().ToArray());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="collection"></param>
        /// <returns></returns>
        protected override string CustomSerialize(params DataRow[] collection)
        {
            StringBuilder sbRows = new StringBuilder();
            DataColumnCollection columns = collection.FirstOrDefault().Table.Columns;
            for (int i = 0; i < collection.Length; i++)
            {
                for (int j = 0; j < columns.Count; j++)
                {
                    string data = null;
                    if (columns[j].DataType == typeof(string))
                    {
                        data = collection[i][columns[j]].ToString().Replace("\r", "").Replace("\n", "");
                    }
                    if (j == columns.Count - 1)
                    {
                        if (!string.IsNullOrWhiteSpace(data))
                        {
                            sbRows.AppendLine(data + ";");
                        }
                        else
                        {
                            sbRows.AppendLine((collection[i][columns[j]]) + ";");
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(data))
                        {
                            sbRows.Append(data).Append(";");
                        }
                        else
                        {
                            sbRows.Append((collection[i][columns[j]])).Append(";");
                        }
                    }
                }
            }
            return sbRows.ToString();
        }

        #endregion

        #region ' Deserialize '

        /// <summary>
        /// 
        /// </summary>
        /// <param name="csvString"></param>
        /// <returns></returns>
        public static DataTable Deserialize(string csvString)
        {
            return (new CsvSerializer()).CustomDeserialize(csvString).ToArray().CopyToDataTable<DataRow>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="csvString"></param>
        /// <returns></returns>
        protected override IEnumerable<DataRow> CustomDeserialize(string csvString)
        {
            DataTable dt = new DataTable();
            string[] csvLines = csvString.Split('\n');
            string[] columnsName = csvLines[0].Split(';');
            for (int i = 0; i < columnsName.Length - 1; i++)
            {
                dt.Columns.Add(columnsName[i]);
            }
            return this.GetArrayOfDataTableRows(dt, csvLines);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="csvLines"></param>
        /// <returns></returns>
        private IEnumerable<DataRow> GetArrayOfDataTableRows(DataTable dt, params string[] csvLines)
        {
            DataRow dr = null;
            for (int i = 0; i < csvLines.Length - 1; i++)
            {
                if (i > 0)
                {
                    dr = dt.NewRow();
                    string[] columnsName = csvLines[0].Split(';');
                    string[] columnsValue = csvLines[i].Split(';');
                    for (int j = 0; j < columnsValue.Length - 1; j++)
                    {
                        dr[columnsName[j]] = columnsValue[j];
                    }
                    yield return dr;
                }
            }
        }

        #endregion

    }

    /// <summary>
    /// 
    /// </summary>
    public sealed partial class CsvSerializer<TEntity> : CsvSerializerAbstraction<TEntity>
        where TEntity : class
    {
        /// <summary>
        /// 
        /// </summary>
        private CsvSerializer()
        {
        }

        #region ' Serialize '

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="collection"></param>
        /// <returns></returns>
        public static string Serialize(List<TEntity> collection)
        {
            return Serialize(collection.ToArray());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="collection"></param>
        /// <returns></returns>
        public static string Serialize(params TEntity[] collection)
        {
            return (new CsvSerializer<TEntity>()).CustomSerialize(collection);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="collection"></param>
        /// <returns></returns>
        protected override string CustomSerialize(params TEntity[] collection)
        {
            StringBuilder sbColumns = new StringBuilder();
            StringBuilder sbRows = new StringBuilder();

            KeyValuePair<PropertyInfo, DataMemberAttribute>[] pairs = this.GetElementsResult((typeof(TEntity))).ToArray();

            this.MountCsvColumns(ref sbColumns, pairs);

            for (int j = 0; j < collection.Length; j++)
            {
                this.MountCsvRows(ref sbRows, collection[j], pairs);
            }
            return sbColumns.ToString() + sbRows.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="sbColumns"></param>
        /// <param name="pairs"></param>
        private void MountCsvColumns(ref StringBuilder sbColumns, params KeyValuePair<PropertyInfo, DataMemberAttribute>[] pairs)
        {
            for (int i = 0; i < pairs.Length; i++)
            {
                if (i == pairs.Length - 1)
                {
                    sbColumns.AppendLine(pairs[i].Value.Name + ";");
                }
                else
                {
                    sbColumns.Append(pairs[i].Value.Name).Append(";");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="sbRows"></param>
        /// <param name="obj"></param>
        /// <param name="pairs"></param>
        private void MountCsvRows(ref StringBuilder sbRows, TEntity obj, params KeyValuePair<PropertyInfo, DataMemberAttribute>[] pairs)
        {
            for (int i = 0; i < pairs.Length; i++)
            {
                string result = (null != obj.GetType().GetProperty(pairs[i].Key.Name).GetValue(obj, null)) ?
                        Convert.ChangeType(obj.GetType().GetProperty(pairs[i].Key.Name).GetValue(obj, null), Type.GetTypeCode(pairs[i].Key.PropertyType)).ToString() : string.Empty;
                result = result.Replace("\r", "").Replace("\n", "");
                if (i == pairs.Length - 1)
                {

                    sbRows.AppendLine(result + ";");
                }
                else
                {
                    sbRows.Append(result).Append(";");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <param name="propsWithoutError"></param>
        /// <returns></returns>
        private bool ContainsPropertyInPropsWithoutExclude(PropertyInfo property, List<PropertyInfo> propsWithoutError)
        {
            bool result = false;
            foreach (PropertyInfo prop in propsWithoutError)
            {
                if (prop.Name == property.Name)
                {
                    result = true;
                }
            }
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ordered"></param>
        /// <param name="properties"></param>
        /// <returns></returns>
        private string MountInnerExcetionMessage(List<DataMemberAttribute> ordered, params PropertyInfo[] properties)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Listing properties without the 'DataMemberAttribute' attribute:");
            List<PropertyInfo> propsWithoutError = new List<PropertyInfo>();

            foreach (DataMemberAttribute item in ordered)
            {
                for (int i = 0; i < properties.Length; i++)
                {
                    DataMemberAttribute att = properties[i].GetCustomAttribute(typeof(DataMemberAttribute)) as DataMemberAttribute;
                    if (null != att && item.Name == att.Name)
                    {
                        propsWithoutError.Add(properties[i]);
                    }
                }
            }

            for (int j = 0; j < properties.Length; j++)
            {
                if (!ContainsPropertyInPropsWithoutExclude(properties[j], propsWithoutError))
                {
                    sb.AppendLine(" '" + properties[j] + "' ");
                }
            }
            return sb.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="type"></param>
        /// <returns></returns>
        private IEnumerable<KeyValuePair<int, DataMemberAttribute>> GetAttributesResult(Type type)
        {
            PropertyInfo[] properties = type.GetProperties();
            for (int i = 0; i < properties.Length; i++)
            {
                DataMemberAttribute att = null;
                try
                {
                    att = properties[i].GetCustomAttribute(typeof(DataMemberAttribute)) as DataMemberAttribute;
                    if (string.IsNullOrWhiteSpace(att.Name))
                    {
                        att = new DataMemberAttribute()
                        {
                            Name = properties[i].Name,
                            Order = att.Order
                        };
                    }
                }
                catch
                {
                    att = new DataMemberAttribute()
                    {
                        Name = properties[i].Name,
                        Order = i
                    };
                }
                yield return new KeyValuePair<int, DataMemberAttribute>(i, att);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        private IEnumerable<KeyValuePair<PropertyInfo, DataMemberAttribute>> GetElementsResult(Type type)
        {
            //TO DO: Alterar para internal
            List<string> adquiredPros = new List<string>();
            List<KeyValuePair<int, DataMemberAttribute>> ordered = this.GetAttributesResult(type).OrderBy(x => x.Value.Order).ToList();
            PropertyInfo[] properties = type.GetProperties();

            foreach (KeyValuePair<int, DataMemberAttribute> pair in ordered)
            {
                yield return new KeyValuePair<PropertyInfo, DataMemberAttribute>(properties[pair.Key], pair.Value);
            }
        }

        #endregion

        #region ' Deserialize '

        /// <summary>
        /// 
        /// </summary>
        /// <param name="csvString"></param>
        /// <returns></returns>
        public static IEnumerable<TEntity> Deserialize(string csvString)
        {
            return (new CsvSerializer<TEntity>()).CustomDeserialize(csvString);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="csvString"></param>
        /// <returns></returns>
        protected override IEnumerable<TEntity> CustomDeserialize(string csvString)
        {
            string[] arrayLinesCsv = csvString.Split('\n');
            string[] columnsName = arrayLinesCsv[0].Split(';');
            Type tp = typeof(TEntity);
            PropertyInfo[] props = tp.GetProperties();
            for (int i = 1; i < arrayLinesCsv.Length - 1; i++)
            {
                object instance = Activator.CreateInstance(tp);
                string[] columnsValue = arrayLinesCsv[i].Split(';');
                for (int j = 0; j < columnsValue.Length - 1; j++)
                {
                    PropertyInfo prop = null;
                    for (int x = 0; x < props.Length; x++)
                    {
                        DataMemberAttribute att = props[x].GetCustomAttribute(typeof(DataMemberAttribute)) as DataMemberAttribute;
                        if ((null != att && att.Name == columnsName[j]) || columnsName[j] == props[x].Name)
                        {
                            prop = props[x];
                        }
                        if (null != prop)
                        {
                            prop.SetValue(instance, Convert.ChangeType(columnsValue[j], Type.GetTypeCode(prop.PropertyType)), null);
                        }
                    }
                }
                yield return (instance as TEntity);
            }
        }

        #endregion

    }
}