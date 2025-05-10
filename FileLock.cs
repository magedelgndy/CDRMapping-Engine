using System.Collections.Generic;

public static class FileLock
{
    private static readonly HashSet<string> LockedFiles = new HashSet<string>();

    public static bool LockFile(string filePath)
    {
        lock (LockedFiles)
        {
            if (LockedFiles.Contains(filePath))
            {
                return false;
            }

            LockedFiles.Add(filePath);
            return true;
        }
    }

    public static void UnlockFile(string filePath)
    {
        lock (LockedFiles)
        {
            if (LockedFiles.Contains(filePath))
            {
                LockedFiles.Remove(filePath);
            }
        }
    }
}
