using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace SFBulkAPIStarter
{
    public class Job
    {
        public String Id { get; set; }
        public String Operation { get; set; }
        public String Object { get; set; }
        public String CreatedById { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime SystemModStamp { get; set; }
        public String State { get; set; }
        public String ConcurrencyMode { get; set; }
        public String ContentType { get; set; }
        public int NumberBatchesQueued { get; set; }
        public int NumberBatchesInProgress { get; set; }
        public int NumberBatchesCompleted { get; set; }
        public int NumberBatchesFailed { get; set; }
        public int NumberBatchesTotal { get; set; }
        public int NumberRecordsProcessed { get; set; }
        public int NumberRecordsFailed { get; set; }
        public int NumberRetries { get; set; }
        public int TotalProcessingTime { get; set; }
        public int ApiActiveProcessingTime { get; set; }
        public int ApexProcessingTime { get; set; }

        public static Job Create(String jobXML)
        {
            XDocument doc = XDocument.Parse(jobXML);

            XElement jobInfoElement = doc.Root;
            List<XElement> jobInfoChildElements = jobInfoElement.Elements().ToList();

            Job job = new Job();

            foreach (XElement e in jobInfoChildElements)
            {
                switch (e.Name.LocalName){
                    case "id":
                        job.Id = e.Value;
                        break;
                    case "operation":
                        job.Operation = e.Value;
                        break;
                    case "object":
                        job.Object = e.Value;
                        break;
                    case "createdById":
                        job.CreatedById = e.Value;
                        break;
                    case "createdDate":
                        job.CreatedDate = DateTime.Parse( e.Value );
                        break;
                    case "systemModstamp":
                        job.SystemModStamp = DateTime.Parse( e.Value );
                        break;
                    case "state":
                        job.State = e.Value;
                        break;
                    case "concurrencyMode":
                        job.ConcurrencyMode = e.Value;
                        break;
                    case "contentType":
                        job.ContentType = e.Value;
                        break;
                    case "numberBatchesQueued":
                        job.NumberBatchesQueued = int.Parse( e.Value );
                        break;
                    case "numberBatchesInProgress":
                        job.NumberBatchesInProgress = int.Parse( e.Value );
                        break;
                    case "numberBatchesCompleted":
                        job.NumberBatchesCompleted = int.Parse( e.Value );
                        break;
                    case "numberBatchesFailed":
                        job.NumberBatchesFailed = int.Parse( e.Value );
                        break;
                    case "numberBatchesTotal":
                        job.NumberBatchesTotal = int.Parse( e.Value );
                        break;
                    case "numberRecordsProcessed":
                        job.NumberRecordsProcessed = int.Parse( e.Value );
                        break;
                    case "numberRetries":
                        job.NumberRetries = int.Parse( e.Value );
                        break;
                    case "numberRecordsFailed":
                        job.NumberRecordsFailed = int.Parse( e.Value );
                        break;
                    case "totalProcessingTime":
                        job.TotalProcessingTime = int.Parse( e.Value );
                        break;
                    case "apiActiveProcessingTime":
                        job.ApiActiveProcessingTime = int.Parse( e.Value );
                        break;
                    case "apexProcessingTime":
                        job.ApexProcessingTime = int.Parse( e.Value );
                        break;
                }
            }

            return job;
        }

        public bool IsDone
        {
            get
            {
                return NumberBatchesTotal == (NumberBatchesCompleted + NumberBatchesFailed) ||
                       State == "Aborted";
            }
        }
    }

    public enum JobOperation
    {
        Query,
        Insert,
        Update,
        Delete,
        HardDelete,
        Upsert
    }

    public enum JobContentType
    {
        CSV,
        XML,
        ZIP_CSV
    }
}
