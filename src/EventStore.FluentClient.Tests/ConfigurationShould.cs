using System;
using System.Configuration;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests
{

    [TestFixture]
    public class ConfigurationShould
    {




        [TestCase("NoIp")]
        [TestCase("NoPort")]
        [TestCase("NoUser")]
        [TestCase("NoPassword")]
        [ExpectedException(typeof(ConfigurationErrorsException))]
        public void ThrowException_WhenRequiredFieldMissing(string name)
        {
            ConfigurationSettings.FromConfig(name);
        }


        [TestCase("InvalidIp1")]
        [TestCase("InvalidIp2")]
        [TestCase("InvalidIp3")]
        [TestCase("InvalidIp4")]
        [ExpectedException(typeof(FormatException))]
        public void ThrowException_WhenIpInvalid(string name)
        {
            ConfigurationSettings.FromConfig(name);
        }

        [TestCase("InvalidPort1")]
        [TestCase("InvalidPort2")]
        [TestCase("InvalidPort3")]
        [TestCase("InvalidPort4")]
        [ExpectedException(typeof(FormatException))]
        public void ThrowException_WhenPortInvalid(string name)
        {
            ConfigurationSettings.FromConfig(name);
        }




        




    }
}
