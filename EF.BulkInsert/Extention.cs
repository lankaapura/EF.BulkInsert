using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Core.Mapping;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.Validation;
using System.Data.SqlClient;
using System.Linq;

namespace EF.BulkInsert
{
    public static class Extention
    {
        public static void BulkInsert<TEntity>(this DbContext context, ICollection<TEntity> entities, int batchSize = 10000, int timeout = 20 * 60)
        {
            try
            {
                var conn = (SqlConnection)context.Database.Connection;

                conn.Open();

                Type t = typeof(TEntity);
                context.Set(t).ToString();
                var objectContext = ((IObjectContextAdapter)context).ObjectContext;
                var workspace = objectContext.MetadataWorkspace;

                var mappings = GetMappings(t, workspace);

                var tableName = GetTableName(t, workspace);

                var bulkCopy = new SqlBulkCopy(conn) { DestinationTableName = tableName };

                // Foreign key relations show up as virtual declared 
                // properties and we want to ignore these.
                var properties = t.GetProperties().Where(p => !p.GetGetMethod().IsVirtual).ToArray();
                var table = new DataTable();
                foreach (var property in properties)
                {
                    Type propertyType = property.PropertyType;

                    // Nullable properties need special treatment.
                    if (propertyType.IsGenericType &&
                        propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        propertyType = Nullable.GetUnderlyingType(propertyType);
                    }

                    // Since we cannot trust the CLR type properties to be in the same order as
                    // the table columns we use the SqlBulkCopy column mappings.
                    table.Columns.Add(new DataColumn(property.Name, propertyType));
                    var clrPropertyName = property.Name;
                    var tableColumnName = mappings[property.Name];
                    bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(clrPropertyName, tableColumnName));
                }

                // Add all our entities to our data table
                foreach (var entity in entities)
                {
                    var e = entity;
                    table.Rows.Add(properties.Select(property => GetPropertyValue(property.GetValue(e, null))).ToArray());
                }

                // send it to the server for bulk execution
                bulkCopy.BatchSize = batchSize;
                bulkCopy.BulkCopyTimeout = timeout;
                bulkCopy.WriteToServer(table);

                conn.Close();
            }
            catch (System.Exception ex)
            {

                throw ex;
            }
        }

        /// <summary>
        /// Gets the property value.
        /// </summary>
        /// <param name="o">The o.</param>
        /// <returns></returns>
        private static object GetPropertyValue(object o)
        {
            if (o == null)
                return DBNull.Value;
            return o;
        }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="metadata">The metadata.</param>
        /// <returns></returns>
        private static string GetTableName(Type type, MetadataWorkspace metadata)
        {
            //var metadata = ((IObjectContextAdapter)context).ObjectContext.MetadataWorkspace;

            // Get the part of the model that contains info about the actual CLR types
            var objectItemCollection = ((ObjectItemCollection)metadata.GetItemCollection(DataSpace.OSpace));

            // Get the entity type from the model that maps to the CLR type
            var entityType = metadata
                    .GetItems<EntityType>(DataSpace.OSpace)
                    .Single(e => objectItemCollection.GetClrType(e) == type);

            // Get the entity set that uses this entity type
            var entitySet = metadata
                .GetItems<EntityContainer>(DataSpace.CSpace)
                .Single()
                .EntitySets
                .Single(s => s.ElementType.Name == entityType.Name);

            // Find the mapping between conceptual and storage model for this entity set
            var mapping = metadata.GetItems<EntityContainerMapping>(DataSpace.CSSpace)
                    .Single()
                    .EntitySetMappings
                    .Single(s => s.EntitySet == entitySet);

            // Find the storage entity set (table) that the entity is mapped
            var table = mapping
                .EntityTypeMappings.Single()
                .Fragments.Single()
                .StoreEntitySet;

            var TypeMapping = mapping
               .EntityTypeMappings.Single()
               .Fragments.Single()
               .TypeMapping;

            var PropertyMappings = mapping
               .EntityTypeMappings.Single()
               .Fragments.Single()
               .PropertyMappings;

            // Return the table name from the storage entity set
            return (string)table.MetadataProperties["Table"].Value ?? table.Name;
        }

        /// <summary>
        /// Gets the mappings.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="metadata">The metadata.</param>
        /// <returns></returns>
        private static Dictionary<string, string> GetMappings(Type type, MetadataWorkspace metadata)
        {
            var mappings = new Dictionary<string, string>();
            //var metadata = ((IObjectContextAdapter)context).ObjectContext.MetadataWorkspace;

            // Get the part of the model that contains info about the actual CLR types
            var objectItemCollection = ((ObjectItemCollection)metadata.GetItemCollection(DataSpace.OSpace));

            // Get the entity type from the model that maps to the CLR type
            var entityType = metadata
                    .GetItems<EntityType>(DataSpace.OSpace)
                    .Single(e => objectItemCollection.GetClrType(e) == type);

            // Get the entity set that uses this entity type
            var entitySet = metadata
                .GetItems<EntityContainer>(DataSpace.CSpace)
                .Single()
                .EntitySets
                .Single(s => s.ElementType.Name == entityType.Name);

            // Find the mapping between conceptual and storage model for this entity set
            var mapping = metadata.GetItems<EntityContainerMapping>(DataSpace.CSSpace)
                    .Single()
                    .EntitySetMappings
                    .Single(s => s.EntitySet == entitySet);

            var PropertyMappings = mapping
                       .EntityTypeMappings.Single()
                       .Fragments.Single()
                       .PropertyMappings;

            foreach (var prop in PropertyMappings)
            {
                var dbcol = (ScalarPropertyMapping)prop;
                mappings.Add(prop.Property.Name, dbcol.Column.Name);
            }

            return mappings;
        }

    }
}
