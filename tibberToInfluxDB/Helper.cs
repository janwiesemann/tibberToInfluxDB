using System.Collections.Generic;

namespace tibberToInfluxDB
{
    internal static class Helper
    {
        public static void AddOrInitialize<T>(ref List<T> list, params T[] add)
        {
            if (list == null)
                list = new List<T>();

            list.AddRange(add);
        }

        public static bool TryGet<T>(this T[] arr, int index, out T res)
        {
            if (index >= arr.Length)
            {
                res = default(T);
                return false;
            }

            res = arr[index];
            return true;
        }
    }
}