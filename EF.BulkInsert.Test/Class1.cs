using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EF.BulkInsert.Test
{
    public class Class1
    {
        TimeSpan time = TimeTakenFor(() =>
        {
            // Do some work
        });

        public static TimeSpan TimeTakenFor(Action action)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            action();
            stopwatch.Stop();
            return stopwatch.Elapsed;
        }
    }
}
