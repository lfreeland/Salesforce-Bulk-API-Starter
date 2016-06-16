using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SFBulkAPIStarter
{
    public class CreateBatchRequest
    {
        public String JobId { get; set; }

        public String BatchContents { get; set; }

        public BatchContentType? BatchContentType { get; set; }

        internal String BatchContentHeader
        {
            get
            {
                switch (BatchContentType)
                {
                    case SFBulkAPIStarter.BatchContentType.CSV:
                        return "text/csv";
                    case SFBulkAPIStarter.BatchContentType.XML:
                        return "application/xml";
                    default:
                        return "text/csv";
                }
            }
        }
    }

    public class CreateAttachmentBatchRequest : CreateBatchRequest
    {
        public String FilePath { get; set; }

        public String ParentId { get; set; }
    }

    public enum BatchContentType
    { 
        CSV,
        XML
    }
}
