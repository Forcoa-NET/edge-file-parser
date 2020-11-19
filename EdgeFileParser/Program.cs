using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

var filename = @"C:\Work\VSB\Data\college-msg\CollegeMsg.txt";

FileStream fileStream = new(filename, FileMode.Open, FileAccess.Read);
List<Edge> edges = new();
HashSet<int> uniqueNodes = new();

Func<List<string>, int, bool> export = (List<string> lines, int snapshotIndex) =>
{
    using (StreamWriter file = new StreamWriter(@$"college-msg-{20*snapshotIndex}.csv"))
    {
        foreach (var l in lines)
        {
            file.WriteLine(l);
        }
    }

    return true;
};

using (var streamReader = new StreamReader(fileStream, Encoding.UTF8))
{
    string line;

    while ((line = streamReader.ReadLine()) != null)
    {
        var values = line.Split(" ");
        var source = Int32.Parse(values[0]);
        var target = Int32.Parse(values[1]);
        var timestamp = TimeSpan.FromTicks(long.Parse(values[2]));

        edges.Add(new Edge { Source = source, Target = target, Date = timestamp });

        if (!uniqueNodes.Contains(source))
        {
            uniqueNodes.Add(source);
        }

        if (!uniqueNodes.Contains(target))
        {
            uniqueNodes.Add(target);
        }
    }
}

var totalNumberOfNodes = uniqueNodes.Count;
var nodesPerThreshold = totalNumberOfNodes / 5;
var uniqueExportedNode = new HashSet<int>();

var linesToExport = new List<string>();
var snapshotIndex = 1;

foreach(var edge in edges.OrderBy(x => x.Date))
{
    linesToExport.Add(edge.ToEdgeLine());

    if(!uniqueExportedNode.Contains(edge.Source))
    {
        uniqueExportedNode.Add(edge.Source);
    }

    if (!uniqueExportedNode.Contains(edge.Target))
    {
        uniqueExportedNode.Add(edge.Target);
    }

    if (uniqueExportedNode.Count >= (nodesPerThreshold*snapshotIndex))
    {
        export(linesToExport, snapshotIndex);
        snapshotIndex++;
    }
}

public class Edge
{
    public int Source { get; init;}
    public int Target { get; init;}
    public TimeSpan Date { get; init;}

    public string ToEdgeLine()
    {
        return $"{Source},{Target},{Date.Ticks}";
    }

}