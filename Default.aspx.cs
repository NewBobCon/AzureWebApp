using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Azure.Storage.Blobs;
using Azure.Data.Tables;

namespace AzureWebApp
{
    public partial class Default : Page
    {
        public static SortedSet<string> columns = new SortedSet<string>();

        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected void Load_Data_Click(object sender, EventArgs e)
        {
            columns.Clear();
            var httpClient = new HttpClient(); //Create an HttpClient object to access dimspey's blob through a REST api call
            string url_blob = "https://css490.blob.core.windows.net/lab4/input.txt"; //Dimpsey's blob storage
            var response = httpClient.GetStringAsync(new Uri(url_blob)).Result; //Grab the object and store the response

            System.Diagnostics.Debug.WriteLine(response);

            BlobServiceClient blob_service = new BlobServiceClient("DefaultEndpointsProtocol=https;AccountName=css436program4blob;AccountKey=E6opY44bYIXe9ZC2oa5/i8UzhDvZMVkmAXtzUEP6r/1J2NoaupgZtKW4j7eGEyGTyu5eVDhKMYusMzOfDfFsRA==;EndpointSuffix=core.windows.net");
            BlobContainerClient blob_container = blob_service.GetBlobContainerClient("newconscontainer");
            System.IO.Stream blob_stream = httpClient.GetStreamAsync(new Uri(url_blob)).Result;
            try
            {
                blob_container.UploadBlob("txt_data", blob_stream); //Try to upload blob, it may already exist
            }
            catch
            {
                blob_container.DeleteBlobIfExists("txt_data"); //Blob already exists so delete it and reupload
                blob_container.UploadBlob("txt_data", blob_stream);
            }

            string url_db = "https://css436-program4-db.table.cosmos.azure.com:443/"; //My Azure Cosmosdb account
            var service_client = new TableServiceClient(new Uri(url_db), new TableSharedKeyCredential("css436-program4-db", "HkWuSOgzrQqcQIfkqrpQimQndkAdZbJPUNt5vQrr3F3Dhy61KA25YgyPqzth8OIJ2c9ptYSbzG7zKxATYvza8w==")); //Create a table service object
            if(service_client.CreateTableIfNotExists("MyProgram4Data") == null) //Create a new table, delete old table if already exists and recreate
            {
                service_client.DeleteTable("MyProgram4Data");
                service_client.CreateTableIfNotExists("MyProgram4Data");
            }

            string data_to_parse = response.ToString(); //String of the data in the text file
            string[] lines = data_to_parse.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries); //Split on new lines into a string array
            foreach(string line in lines) //For every name, split their attributes and place into a table entity
            {
                string[] attributes = line.Split(new char[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                TableEntity entity = new TableEntity();
                entity.PartitionKey = attributes[0];
                entity.RowKey = attributes[1];
                foreach(string attribute in attributes)
                {
                    if (attribute == attributes[0] || attribute == attributes[1]) //Skip last name and first name of attributes
                    {
                        continue;
                    }
                    string[] pair = attribute.Split('=');
                    if(pair[0] == "id")
                    {
                        pair[0] = "id_num";
                    }
                    entity[pair[0]] = pair[1]; //pair[0] is the name of the attribute, pair[1] is the value of that attribute
                    columns.Add(pair[0]);
                }
                TableClient table_client = service_client.GetTableClient("MyProgram4Data");
                table_client.AddEntity(entity); //Push table entity into the table client
            }

            Label_Output.Text = "Loaded data into the blob and the database";
        }

        protected void Clear_Data_Click(object sender, EventArgs e)
        {
            string url_db = "https://css436-program4-db.table.cosmos.azure.com:443/"; //Access my db
            var service_client = new TableServiceClient(new Uri(url_db), new TableSharedKeyCredential("css436-program4-db", "HkWuSOgzrQqcQIfkqrpQimQndkAdZbJPUNt5vQrr3F3Dhy61KA25YgyPqzth8OIJ2c9ptYSbzG7zKxATYvza8w==")); //create a service client
            service_client.DeleteTable("MyProgram4Data"); //Delete the db table

            BlobServiceClient blob_service = new BlobServiceClient("DefaultEndpointsProtocol=https;AccountName=css436program4blob;AccountKey=E6opY44bYIXe9ZC2oa5/i8UzhDvZMVkmAXtzUEP6r/1J2NoaupgZtKW4j7eGEyGTyu5eVDhKMYusMzOfDfFsRA==;EndpointSuffix=core.windows.net"); //Create a storage client
            BlobContainerClient blob_container = blob_service.GetBlobContainerClient("newconscontainer"); //Create a blob client
            blob_container.DeleteBlobIfExists("txt_data"); //Delete the blob

            columns.Clear();

            FName.Text = "";
            LName.Text = "";

            Label_Output.Text = "Cleared data from the blob and the database";
        }

        protected void Query_Click(object sender, EventArgs e)
        {
            Table1.Controls.Clear(); //Empty the table on the webpage
            if(columns.Count() == 0) //This user hasn't loaded the data yet
            {
                Label_Output.Text = "Please load data to query";
                return;
            }
            Label_Output.Text = "Querying database";
            string url_db = "https://css436-program4-db.table.cosmos.azure.com:443/"; //My Azure Cosmosdb account
            var service_client = new TableServiceClient(new Uri(url_db), new TableSharedKeyCredential("css436-program4-db", "HkWuSOgzrQqcQIfkqrpQimQndkAdZbJPUNt5vQrr3F3Dhy61KA25YgyPqzth8OIJ2c9ptYSbzG7zKxATYvza8w==")); //Create a table service object
            TableClient table_client = service_client.GetTableClient("MyProgram4Data");
            Azure.Pageable<TableEntity> query_return; //Pageable query object
            if (FName.Text == "" && LName.Text == "") //Both the first and last name are empty, query all entities in the table
            {
                query_return = table_client.Query<TableEntity>();
            }
            else if (FName.Text == "") //Only the first name is empty, query on the last name value given
            {
                query_return = table_client.Query<TableEntity>(filter: $"PartitionKey eq '{LName.Text}'");
            }
            else if(LName.Text == "") //Only the last name is empty, query on the first name value given
            {
                string my_filter = $"RowKey eq '{FName.Text}'";
                query_return = table_client.Query<TableEntity>(filter : my_filter);
            }
            else //Both the first and last name are given, query this specific value
            {
                string my_filter = $"PartitionKey eq '{LName.Text}'" + " and " + $"RowKey eq '{FName.Text}'";
                query_return = table_client.Query<TableEntity>(filter : my_filter);
            }
            foreach(TableEntity entity in query_return) //For every entity returned, create a row, and a cell for every attribute
            {
                TableRow r = new TableRow();
                TableCell name = new TableCell();
                name.Controls.Add(new LiteralControl(entity.RowKey.ToString() + " " + entity.PartitionKey.ToString())); //Add first and last name as a cell in the row
                r.Cells.Add(name);
                foreach (string column in columns)
                {
                    TableCell c = new TableCell();
                    c.Controls.Add(new LiteralControl(column.ToString() + ": " + entity.GetString(column))); //Add each attribute with their value as a cell in the row
                    r.Cells.Add(c);
                }
                Table1.Rows.Add(r); //Add the row to the webpage's table
                Table1.Width = 1000; //Make the table more readable by defining width and font size
                Table1.Font.Size = 15;
            }
            if(Table1.Rows.Count == 0)
            {
                Label_Output.Text = "Nothing was found in the database that mtaches this query";
            }
            else
            {
                Label_Output.Text = "Here is what was found from your query";
            }
        }
    }
}