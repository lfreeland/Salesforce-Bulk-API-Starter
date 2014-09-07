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
                String[] urlParts = this.Url.Split(new char[] { '.' });
                String podPart = urlParts[0];
                String pod = podPart.ToLower().Replace("https://", String.Empty);

                return pod;
            }
        }
    }
}
