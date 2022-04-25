using System;

namespace tibberToInfluxDB
{
    internal class DataPoint
    {
        public float ConsumptionWatt { get; set; }

        public float Cost { get; set; }

        public string Currency { get; set; }

        public DateTime Date { get; set; }

        public string HomeID { get; set; }

        public string HomeName { get; set; }

        public string Status { get; set; }

        public float UnitPrice { get; set; }

        public float UnitPriceVAT { get; set; }
    }
}