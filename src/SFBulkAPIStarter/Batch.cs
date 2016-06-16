using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace SFBulkAPIStarter
{
    public class Batch
    {
        public String Id { get; set; }
        public String JobId { get; set; }
        public String State { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime SystemModStamp { get; set; }
        public int NumberRecordsProcessed { get; set; }
        public int NumberRecordsFailed { get; set; }
        public int TotalProcessingTime { get; set; }
        public int ApiActiveProcessingTime { get; set; }
        public int ApexProcessingTime { get; set; }

        public static List<Batch> CreateBatches(String batchesResultXML)
        {
            XDocument doc = XDocument.Parse(batchesResultXML);
            XElement batchInfoList = doc.Root;
            List<Batch> batches = new List<Batch>();

            foreach (XNode batchInfoNode in batchInfoList.Nodes())
            {
                Batch batch = Batch.CreateBatch(batchInfoNode.ToString());

                batches.Add(batch);
            }

            return batches;
        }

        public static Batch CreateBatch(string resultXML)
        {
            XDocument doc = XDocument.Parse(resultXML);
            XElement batchInfoElement = doc.Root;
            List<XElement> jobInfoChildElements = batchInfoElement.Elements().ToList();

            Batch batch = new Batch();

            foreach (XElement e in jobInfoChildElements)
            {
                switch (e.Name.LocalName){
                    case "id":
                        batch.Id = e.Value;
                        break;
                    case "jobId":
                        batch.JobId = e.Value;
                        break;
                    case "createdDate":
                        batch.CreatedDate = DateTime.Parse( e.Value );
                        break;
                    case "systemModstamp":
                        batch.SystemModStamp = DateTime.Parse( e.Value );
                        break;
                    case "state":
                        batch.State = e.Value;
                        break;
                    case "numberRecordsProcessed":
                        batch.NumberRecordsProcessed = int.Parse( e.Value );
                        break;
                    case "numberRecordsFailed":
                        batch.NumberRecordsFailed = int.Parse( e.Value );
                        break;
                    case "totalProcessingTime":
                        batch.TotalProcessingTime = int.Parse( e.Value );
                        break;
                    case "apiActiveProcessingTime":
                        batch.ApiActiveProcessingTime = int.Parse( e.Value );
                        break;
                    case "apexProcessingTime":
                        batch.ApexProcessingTime = int.Parse( e.Value );
                        break;
                }
            }

            return batch;
        }
    }
}
