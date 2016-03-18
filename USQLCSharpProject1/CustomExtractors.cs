using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace USQLCSharpProject1
{
    class CustomExtractors
    {
    }
    /// <summary>DOM-based XML applier</summary>
    /// <remarks>Appliers inherit from IApplier and optionally include 
    /// the SqlUserDefinedApplier attribute.
    /// 
    /// They convert a single SQLIP row into a sequence of SQLIP rows.
    /// 
    /// For example, given this row and asked to read the third column:
    /// ("col1", 2, "<![CDATA[<row><a>foo</a><b>3</b></row><row><a/></row>]]>")
    /// 
    /// An applier with the requested schema (a string, b string) produces
    /// ("col1", 2, "foo", "3")
    /// ("col1", 2, "", null)
    /// 
    /// Notice that an empty element produces an empty string,
    /// and a missing element produces null.
    /// 
    /// </remarks>
    [SqlUserDefinedApplier]
    public class XmlApplier : IApplier
    {
        /// <summary>In the input row, the name of the column containing XML. The column must be a string.</summary>
        private string xmlColumnName;

        /// <summary>Path of the XML element that contains rows.</summary>
        private string rowPath;

        /// <summary>For each column, map from the XML path to the column name</summary>
        private SqlMap<string, string> columnPaths;

        /// <summary>New instances are constructed at least once per vertex</summary>
        /// <param name="xmlColumnName">In the input row, the name of the column containing XML. The column must be a string.</param>
        /// <param name="rowPath">Path of the XML element that contains rows.</param>
        /// <param name="columnPaths">For each column, map from the XML path to the column name. 
        /// It is specified relative to the row element.</param>
        /// <remarks>Arguments to appliers must not be column references. 
        /// The arguments must be able to be calculated at compile time.</remarks>
        public XmlApplier(string xmlColumnName, string rowPath, SqlMap<string, string> columnPaths)
        {
            this.xmlColumnName = xmlColumnName;
            this.rowPath = rowPath;
            this.columnPaths = columnPaths;
        }

        /// <summary>Apply is called at least once per instance</summary>
        /// <param name="input">A SQLIP row</param>
        /// <param name="output">A SQLIP updatable row.</param>
        /// <returns>IEnumerable of IRow, one IRow per SQLIP row.</returns>
        /// <remarks>Because applier constructor arguments cannot depend on
        /// column references, the name of the column to parse is given as a string. Then
        /// the actual column value is obtained by calling IRow.Get. The rest of the code
        /// is the same as XmlDomExtractor.</remarks>
        public override IEnumerable<IRow> Apply(IRow input, IUpdatableRow output)
        {
            // Make sure that all requested columns are of type string
            IColumn column = output.Schema.FirstOrDefault(col => col.Type != typeof(string));
            if (column != null)
            {
                throw new ArgumentException(string.Format("Column '{0}' must be of type 'string', not '{1}'", column.Name, column.Type.Name));
            }

            XmlDocument xmlDocument = new XmlDocument();
            xmlDocument.LoadXml(input.Get<string>(this.xmlColumnName));
            foreach (XmlNode xmlNode in xmlDocument.DocumentElement.SelectNodes(this.rowPath))
            {
                // IUpdatableRow implements a builder pattern to save memory allocations, 
                // so call output.Set in a loop
                foreach (IColumn col in output.Schema)
                {
                    var explicitColumnMapping = this.columnPaths.FirstOrDefault(columnPath => columnPath.Value == col.Name);
                    XmlNode xml = xmlNode.SelectSingleNode(explicitColumnMapping.Key ?? col.Name);
                    output.Set(explicitColumnMapping.Value ?? col.Name, xml == null ? null : xml.InnerXml);
                }

                // then call output.AsReadOnly to build an immutable IRow.
                yield return output.AsReadOnly();
            }
        }
    }
    /// <summary>DOM-based XML extractor</summary>
    /// <remarks>Extractors inherit from IExtractor and optionally include 
    /// the SqlUserDefinedExtractor attribute.
    /// 
    /// They convert a sequence of bytes into a sequence of SQLIP rows.
    /// This extractor loads the bytes into a DOM so that it can support
    /// XPath specifications for its rows and columns.
    /// 
    /// For example, given this data and asked to produce the schema (a string, b string):
    /// <![CDATA[<row><a>foo</a><b>3</b></row><row><a/></row>]]>
    /// 
    /// The extractor produces
    /// ("col1", 2, "foo", "3")
    /// ("col1", 2, "", null)
    /// 
    /// Notice that an empty element produces an empty string,
    /// and a missing element produces null.
    /// </remarks>
    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    public class XmlDomExtractor : IExtractor
    {
        /// <summary>Path of the XML elements that contain columns.</summary>
        private string rowPath;

        /// <summary>For each column, map from the XML path to the column name</summary>
        private SqlMap<string, string> columnPaths;

        /// <summary>New instances are constructed at least once per vertex</summary>
        /// <param name="rowPath">Path of the XML element that contains rows.</param>
        /// <param name="columnPaths">For each column, map from the XML path to the column name. 
        /// It is specified relative to the row element.</param>
        /// <remarks>Do not rely on static fields because their values will not cross vertices.</remarks>
        public XmlDomExtractor(string rowPath, SqlMap<string, string> columnPaths)
        {
            this.rowPath = rowPath;
            this.columnPaths = columnPaths;
        }

        /// <summary>Extract is called at least once per vertex</summary>
        /// <param name="input">Wrapper for a Stream</param>
        /// <param name="output">IUpdatableRow uses a mutable builder pattern -- 
        /// set individual fields with IUpdatableRow.Set, then build an immutable IRow by
        /// calling IUpdatableRow.AsReadOnly.</param>
        /// <returns>A sequence of IRows.</returns>
        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            // Make sure that all requested columns are of type string
            IColumn column = output.Schema.FirstOrDefault(col => col.Type != typeof(string));
            if (column != null)
            {
                throw new ArgumentException(string.Format("Column '{0}' must be of type 'string', not '{1}'", column.Name, column.Type.Name));
            }

            XmlDocument xmlDocument = new XmlDocument();
            xmlDocument.Load(input.BaseStream);
            foreach (XmlNode xmlNode in xmlDocument.DocumentElement.SelectNodes(this.rowPath))
            {
                // IUpdatableRow implements a builder pattern to save memory allocations, 
                // so call output.Set in a loop
                foreach (IColumn col in output.Schema)
                {
                    var explicitColumnMapping = this.columnPaths.FirstOrDefault(columnPath => columnPath.Value == col.Name);
                    XmlNode xml = xmlNode.SelectSingleNode(explicitColumnMapping.Key ?? col.Name);
                    output.Set(explicitColumnMapping.Value ?? col.Name, xml == null ? null : xml.InnerXml);
                }

                // then call output.AsReadOnly to build an immutable IRow.
                yield return output.AsReadOnly();
            }
        }
    }
    /// <summary>Streaming XML extractor</summary>
    /// <remarks>Extractors inherit from IExtractor and optionally include 
    /// the SqlUserDefinedExtractor attribute.
    /// 
    /// They convert a sequence of bytes into a sequence of SQLIP rows.
    /// This extractor reads XML incrementally to avoid loading the whole
    /// document into memory. However, it does not support XPath.
    /// 
    /// For example, given this data and asked to produce the schema (a string, b string):
    /// <![CDATA[<row><a>foo</a><b>3</b></row><row><a/></row>]]>
    /// 
    /// The extractor produces
    /// ("col1", 2, "foo", "3")
    /// ("col1", 2, "", null)
    /// 
    /// Notice that an empty element produces an empty string,
    /// and a missing element produces null.
    /// </remarks>
    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    public class XmlExtractor : IExtractor
    {
        /// <summary>Name of the XML element that contains rows.</summary>
		private string rowPath;

        /// <summary>For each column, map from the XML element name to the column name</summary>
        private SqlMap<string, string> columnPaths;

        /// <summary>New instances are constructed at least once per vertex</summary>
        /// <param name="rowElement">Name of the XML element that contains rows.</param>
        /// <param name="columnElements">For each column, map from the XML element name to the column name</param>
        /// <remarks>Do not rely on static fields because their values are not shared across vertices.</remarks>
        public XmlExtractor(string rowPath, SqlMap<string, string> columnPaths)
        {
            this.rowPath = rowPath;
            this.columnPaths = columnPaths;
        }

        /// <summary>The state names in the XML parser finite-state machine.</summary>
        private enum ParseLocation
        {
            Row,
            Column,
            Data
        }

        /// <summary>The current state in the XML parser finite-state machine.</summary>
        private class ParseState
        {
            /// <summary>The current location in the finite-state machine.</summary>
            public ParseLocation Location { get; set; }

            /// <summary>The current element name.</summary>
            /// <remarks>It will map to a column when its value is known.</remarks>
            public string ElementName { get; set; }

            /// <summary>XML writer for the current element value.</summary>
            /// <remarks>It is built up from the inner XML of an element, then written to a column.</remarks>
            public XmlWriter ElementWriter { get; set; }

            /// <summary>The current element value.</summary>
            private StringBuilder elementValue;

            /// <summary>Set up the element writer state when constructing a ParseState</summary>
            public ParseState()
            {
                this.elementValue = new StringBuilder();
                this.ElementWriter = XmlWriter.Create(
                    this.elementValue,
                    new XmlWriterSettings { ConformanceLevel = ConformanceLevel.Fragment });
            }

            /// <summary>Jump to a different location and clear the currrent element buffer.</summary>
            public void ClearAndJump(ParseLocation location)
            {
                this.Location = location;
                this.ElementName = null;
                this.ClearElementValue();
            }

            /// <summary>Get the current element value and clear its buffer</summary>
            public string ReadElementValue()
            {
                this.ElementWriter.Flush();
                string s = this.elementValue.ToString();
                this.elementValue.Clear();
                return s;
            }

            /// <summary>Clear the buffer used for reading the current element value</summary>
            public void ClearElementValue()
            {
                this.ElementWriter.Flush();
                this.elementValue.Clear();
            }
        }

        /// <summary>Extract is called at least once per instance</summary>
        /// <param name="input">Wrapper for a Stream</param>
        /// <param name="output">IUpdatableRow uses a mutable builder pattern -- 
        /// set individual fields with IUpdatableRow.Set, then build an immutable IRow by
        /// calling IUpdatableRow.AsReadOnly.</param>
        /// <returns>IEnumerable of IRow, one IRow per SQLIP row.</returns>
		public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            // Make sure that all requested columns are of type string
            IColumn column = output.Schema.FirstOrDefault(col => col.Type != typeof(string));
            if (column != null)
            {
                throw new ArgumentException(string.Format("Column '{0}' must be of type 'string', not '{1}'", column.Name, column.Type.Name));
            }

            var state = new ParseState();
            state.ClearAndJump(ParseLocation.Row);
            using (var reader = XmlReader.Create(input.BaseStream))
            {
                while (reader.Read())
                {
                    switch (state.Location)
                    {
                        case ParseLocation.Row:
                            // when looking for a new row, we are only interested in elements
                            // whose name matches the requested row element
                            if (reader.NodeType == XmlNodeType.Element && reader.Name == this.rowPath)
                            {
                                // when found, clear the IUpdatableRow's memory
                                // (this is no provided Clear method)
                                for (int i = 0; i < output.Schema.Count; i++)
                                {
                                    output.Set<string>(i, null);
                                }

                                state.ClearAndJump(ParseLocation.Column);
                            }

                            break;
                        case ParseLocation.Column:
                            // When looking for a new column, we are interested in elements
                            // whose name is a key in the columnPaths map or
                            // whose name is in the requested output schema.
                            // This indicates a column whose value needs to be read, 
                            // so prepare for reading it by clearing elementValue.
                            if (reader.NodeType == XmlNodeType.Element
                                && (this.columnPaths.ContainsKey(reader.Name)
                                    || output.Schema.Select(c => c.Name).Contains(reader.Name)))
                            {
                                if (reader.IsEmptyElement)
                                {
                                    // For an empty element, set an empty string 
                                    // and immediately jump to looking for the next column
                                    output.Set(this.columnPaths[reader.Name] ?? reader.Name, state.ReadElementValue());
                                    state.ClearAndJump(ParseLocation.Column);
                                }
                                else
                                {
                                    state.Location = ParseLocation.Data;
                                    state.ElementName = reader.Name;
                                    state.ClearElementValue();
                                }
                            }
                            else if (reader.NodeType == XmlNodeType.EndElement && reader.Name == this.rowPath)
                            {
                                // The other interesting case is an end element whose name matches 
                                // the current row element. This indicates the end of a row, 
                                // so yield the now-complete row and jump to looking for 
                                // another row.
                                yield return output.AsReadOnly();
                                state.ClearAndJump(ParseLocation.Row);
                            }

                            break;
                        case ParseLocation.Data:
                            // Most of the code for reading the value of a column
                            // deals with re-creating the inner XML from discrete elements.
                            // The only jump occurs when the reader hits an end element
                            // whose name matches the current column. In this case, we
                            // need to write the accumulated value to the appropriate 
                            // column in the output row.
                            switch (reader.NodeType)
                            {
                                case XmlNodeType.EndElement:
                                    if (reader.Name == state.ElementName)
                                    {
                                        output.Set(this.columnPaths[state.ElementName] ?? state.ElementName, state.ReadElementValue());
                                        state.ClearAndJump(ParseLocation.Column);
                                    }
                                    else
                                    {
                                        state.ElementWriter.WriteEndElement();
                                    }

                                    break;
                                case XmlNodeType.Element:
                                    state.ElementWriter.WriteStartElement(reader.Name);
                                    state.ElementWriter.WriteAttributes(reader, false);
                                    if (reader.IsEmptyElement)
                                    {
                                        state.ElementWriter.WriteEndElement();
                                    }

                                    break;
                                case XmlNodeType.CDATA:
                                    state.ElementWriter.WriteCData(reader.Value);
                                    break;
                                case XmlNodeType.Comment:
                                    state.ElementWriter.WriteComment(reader.Value);
                                    break;
                                case XmlNodeType.ProcessingInstruction:
                                    state.ElementWriter.WriteProcessingInstruction(reader.Name, reader.Value);
                                    break;
                                default:
                                    state.ElementWriter.WriteString(reader.Value);
                                    break;
                            }

                            break;
                        default:
                            throw new NotImplementedException("StreamFromXml has not implemented a new member of the ParseLocation enum");
                    }
                }

                if (state.Location != ParseLocation.Row)
                {
                    throw new ArgumentException("XML document ended without proper closing tags");
                }
            }
        }
    }
    /// <summary>XML outputter</summary>
    /// <remarks>Outputters inherit from IOutputter and optionally include 
    /// the SqlUserDefinedOutputter attribute.
    /// 
    /// They write a single SQLIP row to a byte stream. Given a SQLIP rowset,
    /// the XML outputter produces a sequence of XML fragments because of this,
    /// one fragment per row.
    /// 
    /// For example, given this row and the schema (a string, b string, c string):
    /// ("1", "foo", "bar")
    /// ("2", null, "")
    /// 
    /// The outputter will produce
    /// <![CDATA[
    /// <row><a>1</a><b>foo</b><c>bar</c></row>
    /// <row><a>2</a><c/></row>]]>
    /// 
    /// Notice that an empty string produces an empty element,
    /// and null produces a missing element.
    /// Notice that this outputter doesn't require atomic output,
    /// since it produces xml fragments as opposed to a root node
    /// </remarks>
    [SqlUserDefinedOutputter(AtomicFileProcessing = false)]
    public class XmlOutputter : IOutputter
    {
        /// <summary>Name of the XML element that will contain columns from a single row.</summary>
        private string rowPath;

        /// <summary>For each column, map from the column name to the XML element name</summary>
        private SqlMap<string, string> columnPaths;

        /// <summary>Settings for the XML writer</summary>
        /// <remarks>Because IOuputters output one row at a time, this code
        /// outputs XML fragments -- one per row -- instead of a single document.</remarks>
        private XmlWriterSettings fragmentSettings = new XmlWriterSettings
        {
            ConformanceLevel = ConformanceLevel.Fragment,
            OmitXmlDeclaration = true
        };

        /// <summary>New instances are created at least once per vertex</summary>
        /// <param name="rowPath">Name of the XML element that will contain columns from a single row.</param>
        /// <remarks>The column names from the input rowset will be used as the column element names.
        /// Do not rely on static fields because their values are not shared across vertices.</remarks>
        public XmlOutputter(string rowPath)
            : this(rowPath, new SqlMap<string, string>())
        {
        }

        /// <summary>New instances are created at least once per vertex</summary>
        /// <param name="rowPath">Name of the XML element that will contain columns from a single row.</param>
        /// <param name="columnElements">For each column, map from the column name to the XML element name</param>
        /// <remarks>Do not rely on static fields because their values are not shared across vertices.</remarks>
        public XmlOutputter(string rowPath, SqlMap<string, string> columnPaths)
        {
            this.rowPath = rowPath;
            this.columnPaths = columnPaths;
        }

        /// <summary>Output is called at least once per instance</summary>
        /// <param name="input">A SQLIP row</param>
        /// <param name="output">Wrapper for a Stream</param>
        public override void Output(IRow input, IUnstructuredWriter output)
        {
            IColumn badColumn = input.Schema.FirstOrDefault(col => col.Type != typeof(string));
            if (badColumn != null)
            {
                throw new ArgumentException(string.Format("Column '{0}' must be of type 'string', not '{1}'", badColumn.Name, badColumn.Type.Name));
            }

            using (var writer = XmlWriter.Create(output.BaseStream, this.fragmentSettings))
            {
                writer.WriteStartElement(this.rowPath);
                foreach (IColumn col in input.Schema)
                {
                    var value = input.Get<string>(col.Name);
                    if (value != null)
                    {
                        // Skip null values in order to distinguish them from empty strings
                        writer.WriteElementString(this.columnPaths[col.Name] ?? col.Name, value);
                    }
                }
            }
        }
    }
    /// <summary>The XPath functions provide XPath 1.0 querying on strings.</summary>
    /// <remarks>Unlike Hive, the XPath query is not cached per-statement.
    /// You can cache it yourself with a DECLARE statement:
    ///     DECLARE @xpath = "a/b";
    ///     SELECT XPath.String(xmlColumn, @xpath) AS stringValue FROM table;</remarks>
    public static class XPath
    {
        /// <summary>Return an array of strings containing XML that match the XPath query</summary>
        /// <param name="xml">String containing XML</param>
        /// <param name="xpath">XPath query</param>
        /// <returns>Array of strings containing XML that match the xpath query</returns>
        /// <remarks>The query returns XmlNode.InnerXml, so attribute text is not returned</remarks>
        public static SqlArray<string> FindNodes(string xml, string xpath)
        {
            if (string.IsNullOrEmpty(xml))
            {
                return new SqlArray<string>();
            }

            return FindNodes(Load(xml), xpath);
        }

        /// <summary>Return an array of strings containing XML that match the XPath query</summary>
        /// <param name="xml">String containing XML</param>
        /// <param name="xpath">XPath query</param>
        /// <returns>Array of strings containing XML that match the xpath query</returns>
        /// <remarks>The query returns XmlNode.InnerXml, so attribute text is not returned</remarks>
        public static SqlArray<SqlArray<string>> FindNodes(string xml, params string[] xpaths)
        {
            if (string.IsNullOrEmpty(xml) || xpaths.Length == 0)
            {
                return new SqlArray<SqlArray<string>>();
            }

            XmlNode doc = Load(xml);
            return new SqlArray<SqlArray<string>>(xpaths.Select(xpath => FindNodes(doc, xpath)));
        }

        /// <summary>Return an array of strings containing text that match the XPath query</summary>
        /// <param name="xml">String containing XML</param>
        /// <param name="xpath">XPath query</param>
        /// <returns>Array of strings that match the xpath query</returns>
        /// <remarks>The query must return text nodes or attributes -- 
        /// otherwise this function returns an empty array.</remarks>
        public static SqlArray<string> Evaluate(string xml, string xpath)
        {
            if (string.IsNullOrEmpty(xml))
            {
                return new SqlArray<string>();
            }

            return Evaluate(Load(xml), xpath);
        }

        /// <summary>Return an array of array of strings that match multiple XPath queries</summary>
        /// <param name="xml">String containing XML</param>
        /// <param name="xpath">XPath query</param>
        /// <returns>Array of array of strings that match the xpath query</returns>
        /// <remarks>The queries must return text nodes or attributes -- 
        /// otherwise this function returns an empty array for that query.</remarks>
        public static SqlArray<SqlArray<string>> Evaluate(string xml, params string[] xpaths)
        {
            if (string.IsNullOrEmpty(xml) || xpaths.Length == 0)
            {
                return new SqlArray<SqlArray<string>>();
            }

            XmlNode doc = Load(xml);
            return new SqlArray<SqlArray<string>>(xpaths.Select(xpath => Evaluate(doc, xpath)));
        }

        /// <summary>Return an array of strings that match the XPath query</summary>
        /// <param name="root">Root of the XML to query</param>
        /// <param name="xpath">XPath query</param>
        /// <returns>Array of strings that match the xpath query</returns>
        /// <remarks>The query must return text nodes or attributes -- 
        /// otherwise this function returns an empty array.</remarks>
        private static SqlArray<string> Evaluate(XmlNode root, string xpath)
        {
            var nodes = root.SelectNodes(xpath).Cast<XmlNode>();
            if (nodes.All(node => node.NodeType == XmlNodeType.Text || node.NodeType == XmlNodeType.Attribute))
            {
                return new SqlArray<string>(nodes.Select(node => node.InnerText));
            }
            else
            {
                return new SqlArray<string>();
            }
        }

        /// <summary>Return an array of strings that match the XPath query</summary>
        /// <param name="root">Root of the XML to query</param>
        /// <param name="xpath">XPath query</param>
        /// <returns>Array of strings that match the xpath query</returns>
        /// <remarks>The query must return text nodes or attributes -- 
        /// otherwise this function returns an empty array.</remarks>
        private static SqlArray<string> FindNodes(XmlNode root, string xpath)
        {
            return new SqlArray<string>(root.SelectNodes(xpath).Cast<XmlNode>().Select(node => node.InnerXml));
        }

        /// <summary>Utility to load XML from a string</summary>
        private static XmlNode Load(string xml)
        {
            var d = new XmlDocument();
            d.LoadXml(xml);
            return d;
        }
    }
    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    public class XmlDomExtractorNs : IExtractor
    {
        private string rowPath;
        private SqlMap<string, string> columnPaths;
        private string namespaces;
        private Regex xmlns = new Regex("(?:xmlns:)?(\\S+)\\s*=\\s*([\"']?)(\\S+)\\2");

        public XmlDomExtractorNs(string rowPath, SqlMap<string, string> columnPaths, string namespaces)
        {
            this.rowPath = rowPath;
            this.columnPaths = columnPaths;
            this.namespaces = namespaces;
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            IColumn column = output.Schema.FirstOrDefault(col => col.Type != typeof(string));
            if (column != null)
            {
                throw new ArgumentException(string.Format("Column '{0}' must be of type 'string', not '{1}'", column.Name, column.Type.Name));
            }

            XmlDocument xmlDocument = new XmlDocument();
            xmlDocument.Load(input.BaseStream);

            XmlNamespaceManager nsmgr = new XmlNamespaceManager(xmlDocument.NameTable);
            if (this.namespaces != null)
            {
                foreach (Match nsdef in xmlns.Matches(this.namespaces))
                {
                    string prefix = nsdef.Groups[1].Value;
                    string uri = nsdef.Groups[3].Value;
                    nsmgr.AddNamespace(prefix, uri);
                }
            }

            foreach (XmlNode xmlNode in xmlDocument.DocumentElement.SelectNodes(this.rowPath, nsmgr))
            {
                foreach (IColumn col in output.Schema)
                {
                    var explicitColumnMapping = this.columnPaths.FirstOrDefault(columnPath => columnPath.Value == col.Name);
                    XmlNode xml = xmlNode.SelectSingleNode(explicitColumnMapping.Key ?? col.Name, nsmgr);
                    output.Set(explicitColumnMapping.Value ?? col.Name, xml == null ? null : xml.InnerXml);
                }
                yield return output.AsReadOnly();
            }
        }
    }
}