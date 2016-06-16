using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace SFBulkAPIStarter
{
    /// <summary>
    /// Wrapper class to Salesforce's Bulk API.
    /// </summary>
    /// <seealso cref="https://www.salesforce.com/us/developer/docs/api_asynch/"/>

    public class BulkApiClient
    {
        private String _UserName { get; set; }
        private String _Password { get; set; }
        private String _LoginURL { get; set; }

        SFEnterprise.SforceService _sfService = null;
        SFEnterprise.LoginResult _loginResult = null;

        public BulkApiClient(String username, String password, String loginUrl)
        {
            _UserName = username;
            _Password = password;
            _LoginURL = loginUrl;

            Login();
        }

        public Job CreateJob(CreateJobRequest createJobRequest)
        {
            String jobRequestXML =
            @"<?xml version=""1.0"" encoding=""UTF-8""?>
             <jobInfo xmlns=""http://www.force.com/2009/06/asyncapi/dataload"">
               <operation>{0}</operation>
               <object>{1}</object>
               {3}
               <contentType>{2}</contentType>
             </jobInfo>";

            String externalField = String.Empty;

            if (String.IsNullOrWhiteSpace(createJobRequest.ExternalIdFieldName) == false)
            {
                externalField = "<externalIdFieldName>" + createJobRequest.ExternalIdFieldName + "</externalIdFieldName>";
            }

            jobRequestXML = String.Format(jobRequestXML,
                                          createJobRequest.OperationString,
                                          createJobRequest.Object,
                                          createJobRequest.ContentTypeString,
                                          externalField);

            String createJobUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job";

            String resultXML = invokeRestAPI(createJobUrl, jobRequestXML); 

            return Job.Create(resultXML);
        }

        public Job CloseJob(String jobId)
        {
            String closeJobUrl = buildSpecificJobUrl(jobId);
            String closeRequestXML = 
            @"<?xml version=""1.0"" encoding=""UTF-8""?>" + Environment.NewLine +
            @"<jobInfo xmlns=""http://www.force.com/2009/06/asyncapi/dataload"">" + Environment.NewLine +
             "<state>Closed</state>" + Environment.NewLine +
             "</jobInfo>";

            String resultXML = invokeRestAPI(closeJobUrl, closeRequestXML);

            return Job.Create(resultXML);
        }

        public Job GetJob(String jobId)
        {
            String getJobUrl = buildSpecificJobUrl(jobId);

            String resultXML = invokeRestAPI(getJobUrl);

            return Job.Create(resultXML);
        }

        public Job GetCompletedJob(String jobId)
        {
            Job job = GetJob(jobId);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = job = GetJob(jobId);
            }

            return job;
        }

        public Batch CreateAttachmentBatch(CreateAttachmentBatchRequest request)
        {
            String requestTxtFileCSVContents = "Name,ParentId,Body" + Environment.NewLine;
            requestTxtFileCSVContents += request.FilePath + "," + request.ParentId + ",#" + request.FilePath;

            using (MemoryStream memoryStream = new MemoryStream())
            {
                using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
                {
                    byte[] requestTxtFileCSVContentsBytes = UTF8Encoding.UTF8.GetBytes(requestTxtFileCSVContents);
                    var requestTxtFileInArchive = archive.CreateEntry("request.txt");
                    using (var entryStream = requestTxtFileInArchive.Open())
                    using (var fileToCompressStream = new MemoryStream(requestTxtFileCSVContentsBytes))
                    {
                        fileToCompressStream.CopyTo(entryStream);
                    }

                    byte[] attachmentFileContentsBytes = File.ReadAllBytes(request.FilePath);
                    var attachmentFileInArchive = archive.CreateEntry(request.FilePath);
                    using (var attachmentEntryStream = attachmentFileInArchive.Open())
                    using (var attachmentFileToCompressStream = new MemoryStream(attachmentFileContentsBytes))
                    {
                        attachmentFileToCompressStream.CopyTo(attachmentEntryStream);
                    }

                    byte[] zipFileBytes = memoryStream.ToArray();

                    String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job/" + request.JobId + "/batch";

                    byte[] responseBytes = invokeRestAPI(requestUrl, zipFileBytes, "POST", "zip/csv");

                    String resultXML = UTF8Encoding.UTF8.GetString(responseBytes);

                    return Batch.CreateBatch(resultXML);
                }
            }
        }

        public Batch CreateBatch(CreateBatchRequest createBatchRequest){
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job/" + createBatchRequest.JobId + "/batch";

            String requestXML = createBatchRequest.BatchContents;

            String contentType = String.Empty;

            if (createBatchRequest.BatchContentType.HasValue)
            {
                contentType = createBatchRequest.BatchContentHeader;
            }

            String resultXML = invokeRestAPI(requestUrl, requestXML, "Post", contentType);

            return Batch.CreateBatch(resultXML);
        }

        public Batch GetBatch(string jobId, string batchId)
        {
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job/" + jobId + "/batch/" + batchId;

            String resultXML = invokeRestAPI(requestUrl);

            return Batch.CreateBatch(resultXML);
        }

        public List<Batch> GetBatches(String jobId){
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job/" + jobId + "/batch/";

            String resultXML = invokeRestAPI(requestUrl);

            return Batch.CreateBatches(resultXML);
        }

        public String GetBatchRequest(String jobId, String batchId)
        {
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job/" + jobId + "/batch/" + batchId + "/request";

            String resultXML = invokeRestAPI(requestUrl);

            return resultXML;
        }

        public String GetBatchResults(String jobId, String batchId)
        {
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job/" + jobId + "/batch/" + batchId + "/result";

            String resultXML = invokeRestAPI(requestUrl);

            return resultXML;
        }

        public List<String> GetResultIds(String queryBatchResultListXML)
        {
            //<result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload"><result>752x000000000F1</result></result-list>

            XDocument doc = XDocument.Parse(queryBatchResultListXML);
            List<String> resultIds = new List<string>();

            XElement resultListElement = doc.Root;

            foreach (XElement resultElement in resultListElement.Elements())
            {
                String resultId = resultElement.Value;
                resultIds.Add(resultId);
            }

            return resultIds;
        }

        public String GetBatchResult(String jobId, String batchId, String resultId)
        {
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job/" + jobId + "/batch/" + batchId + "/result/" + resultId;

            String resultXML = invokeRestAPI(requestUrl);

            return resultXML;
        }

        private String buildSpecificJobUrl(String jobId)
        {
            return "https://" + _sfService.Pod + ".salesforce.com/services/async/31.0/job/" + jobId;
        }

        private void Login()
        {
            _sfService = new SFEnterprise.SforceService();

            _sfService.Url = _LoginURL;
            _loginResult = _sfService.login(_UserName, _Password);
            _sfService.Url = _loginResult.serverUrl;
        }

        private String invokeRestAPI(String endpointURL)
        {
            WebClient wc = buildWebClient();

            return wc.DownloadString(endpointURL);
        }

        private String invokeRestAPI(String endpointURL, String postData)
        {
            return invokeRestAPI(endpointURL, postData, "Post", String.Empty);
        }

        private String invokeRestAPI(String endpointURL, String postData, String httpVerb, String contentType)
        {
            byte[] postDataBytes = UTF8Encoding.UTF8.GetBytes(postData);

            byte[] response = invokeRestAPI(endpointURL, postDataBytes, httpVerb, contentType);

            return UTF8Encoding.UTF8.GetString(response);
        }

        private byte[] invokeRestAPI(String endpointURL, byte[] postData, String httpVerb, String contentType)
        {
            WebClient wc = buildWebClient();

            if (String.IsNullOrWhiteSpace(contentType) == false)
            {
                wc.Headers.Add("Content-Type: " + contentType);
            }

            try
            {
                return wc.UploadData(endpointURL, httpVerb, postData);
            }
            catch (WebException webEx)
            {
                String error = String.Empty;

                if (webEx.Response != null)
                {
                    using (var errorResponse = (HttpWebResponse)webEx.Response)
                    {
                        using (var reader = new StreamReader(errorResponse.GetResponseStream()))
                        {
                            error = reader.ReadToEnd();
                        }
                    }
                }

                throw;
            }
        }

        private WebClient buildWebClient()
        {
            WebClient wc = new WebClient();
            wc.Encoding = Encoding.UTF8;
            wc.Headers.Add("X-SFDC-Session: " + _loginResult.sessionId);

            return wc;
        }
    }
}
