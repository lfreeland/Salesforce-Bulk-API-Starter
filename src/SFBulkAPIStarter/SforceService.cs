using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SFBulkAPIStarter.SFEnterprise
{
    public partial class SforceService
    {
        public String Pod
        {
            get
            {
                String podPart = "";
                int i = 0;
                String[] urlParts = this.Url.Split(new char[] { '.' });
                while (!urlParts[i].Contains("salesforce"))
                {
                    podPart += urlParts[i] + ".";
                    i++;
                }
                podPart = podPart.Substring(0, podPart.Length - 1);
                String pod = podPart.ToLower().Replace("https://", String.Empty);

                return pod;
            }
        }
    }
}
