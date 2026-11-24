/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;

/// <summary>
/// Builds the public .NET projects of the Ignite source tree in the current directory and prints their public API, one
/// declaration per line: area, key, value, flags and location, separated by tabs. The projects and the namespaces left out
/// come from the docfx configuration that builds the API documentation. The assemblies are read as metadata, so their
/// dependencies are not loaded.
/// </summary>
internal static class Program
{
    /** */
    private const string Dotnet = "modules/platforms/dotnet";

    /** docfx configuration: the projects of the API documentation. */
    private const string Docfx = Dotnet + "/docfx/Apache.Ignite.docfx.json";

    /** docfx filter: the namespaces left out of the API documentation. */
    private const string Filter = Dotnet + "/docfx/filterConfig.yml";

    private static int Main()
    {
        var root = Directory.GetCurrentDirectory();
        var excluded = File.ReadAllLines(Path.Combine(root, Filter))
            .Select(l => l.Trim())
            .Where(l => l.StartsWith("uidRegex:"))
            .Select(l => new Regex(l.Substring("uidRegex:".Length).Trim()))
            .ToList();
        var count = 0;

        foreach (var project in Projects(root))
        {
            Build(project);

            count += new Describer(root, project, excluded).Describe(Console.Out);
        }

        if (count == 0)
        {
            Console.Error.WriteLine("No public .NET declarations found");

            return 1;
        }

        return 0;
    }

    /// <summary>Returns the project files that docfx documents.</summary>
    private static IEnumerable<string> Projects(string root)
    {
        var options = new JsonDocumentOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(root, Docfx)), options);
        var names = doc.RootElement.GetProperty("metadata").EnumerateArray()
            .SelectMany(m => m.GetProperty("src").EnumerateArray())
            .SelectMany(s => s.GetProperty("files").EnumerateArray())
            .Select(f => Path.GetFileName(f.GetString()))
            .ToList();
        var projects = names
            .Select(n => Directory.GetFiles(Path.Combine(root, Dotnet), n, SearchOption.AllDirectories)
                .Single(p => !p.Contains("/obj/") && !p.Contains("/bin/")))
            .ToList();

        if (projects.Count == 0)
            throw new InvalidOperationException("No projects in " + Docfx);

        return projects;
    }

    /// <summary>Builds a project. Its output goes to the error stream, so the declarations stay alone on the output.</summary>
    private static void Build(string project)
    {
        var info = new ProcessStartInfo("dotnet", $"build \"{project}\" -c Release -nologo -v q")
        {
            RedirectStandardOutput = true,
        };

        using var process = Process.Start(info);

        Console.Error.Write(process.StandardOutput.ReadToEnd());
        process.WaitForExit();

        if (process.ExitCode != 0)
            throw new InvalidOperationException("Could not build " + project);
    }
}

/// <summary>Describes the public and protected API of one assembly.</summary>
internal sealed class Describer : ISignatureTypeProvider<string, object>
{
    /** */
    private const string Area = ".NET API";

    /** */
    private readonly string _root;

    /** */
    private readonly MetadataReader _md;

    /** Debug information with the source lines of method bodies. */
    private readonly MetadataReader _pdb;

    /** */
    private readonly List<Regex> _excluded;

    /** Source files of the project by type name. Ignite.NET declares one type per file named after it. */
    private readonly Dictionary<string, string> _files = new Dictionary<string, string>();

    /** */
    private TextWriter _out;

    /** */
    private int _count;

    public Describer(string root, string project, List<Regex> excluded)
    {
        _root = root;
        _excluded = excluded;

        var xml = XDocument.Load(project);
        var dir = Path.GetDirectoryName(project);
        var name = xml.Descendants("AssemblyName").FirstOrDefault()?.Value ?? Path.GetFileNameWithoutExtension(project);
        var framework = xml.Descendants("TargetFramework").First().Value;
        var dll = Path.Combine(dir, "bin", "Release", framework, name + ".dll");

        _md = new PEReader(File.OpenRead(dll)).GetMetadataReader();
        _pdb = MetadataReaderProvider.FromPortablePdbStream(File.OpenRead(Path.ChangeExtension(dll, ".pdb"))).GetMetadataReader();

        foreach (var f in Directory.GetFiles(dir, "*.cs", SearchOption.AllDirectories)
                     .Where(p => !p.Contains("/obj/") && !p.Contains("/bin/")))
            _files.TryAdd(Path.GetFileNameWithoutExtension(f), f);
    }

    /// <summary>Prints the declarations and returns their number.</summary>
    public int Describe(TextWriter output)
    {
        _out = output;

        foreach (var h in _md.TypeDefinitions)
        {
            var type = _md.GetTypeDefinition(h);

            if (Visible(type) && !Excluded(TypeName(h)))
                DescribeType(h, type);
        }

        return _count;
    }

    /// <summary>Returns true if the type is in a namespace that the API documentation leaves out, such as an Impl namespace.</summary>
    private bool Excluded(string typeName) => _excluded.Any(r => r.IsMatch(typeName + "."));

    /// <summary>Returns true if users can reference the type.</summary>
    private bool Visible(TypeDefinition type)
    {
        var vis = type.Attributes & TypeAttributes.VisibilityMask;

        if (!type.GetDeclaringType().IsNil)
        {
            var owner = _md.GetTypeDefinition(type.GetDeclaringType());

            return Visible(owner) && (vis == TypeAttributes.NestedPublic
                || (vis == TypeAttributes.NestedFamily || vis == TypeAttributes.NestedFamORAssem) && !Sealed(owner));
        }

        return vis == TypeAttributes.Public;
    }

    /** */
    private static bool Sealed(TypeDefinition type) => (type.Attributes & TypeAttributes.Sealed) != 0;

    /// <summary>Returns true if users can reach a member with this access in the type.</summary>
    private static bool Accessible(int access, TypeDefinition owner) =>
        access == (int)MethodAttributes.Public
        || (access == (int)MethodAttributes.Family || access == (int)MethodAttributes.FamORAssem) && !Sealed(owner);

    /** */
    private void DescribeType(TypeDefinitionHandle h, TypeDefinition type)
    {
        var name = TypeName(h);
        var where = TypeLocation(h);
        var obsolete = Obsolete(type.GetCustomAttributes());
        var baseType = type.BaseType.IsNil ? null : Name(type.BaseType);
        var kind = (type.Attributes & TypeAttributes.Interface) != 0 ? "interface"
            : baseType == "System.Enum" ? "enum"
            : baseType == "System.ValueType" ? "struct"
            : baseType == "System.MulticastDelegate" ? "delegate"
            : "class";
        var mods = kind != "class" ? ""
            : (type.Attributes & TypeAttributes.Abstract) != 0 && Sealed(type) ? "static "
            : (type.Attributes & TypeAttributes.Abstract) != 0 ? "abstract "
            : Sealed(type) ? "sealed " : "";
        var supers = new List<string>();

        if (kind == "class" && baseType != null && baseType != "System.Object")
            supers.Add(baseType);

        // Interfaces from the namespaces left out of the API are internal: users see only the members that the type declares.
        supers.AddRange(type.GetInterfaceImplementations().Select(i => Name(_md.GetInterfaceImplementation(i).Interface))
            .Where(s => !Excluded(s)).OrderBy(s => s));

        if (kind == "delegate")
        {
            var invoke = type.GetMethods().Select(m => _md.GetMethodDefinition(m)).Single(m => _md.GetString(m.Name) == "Invoke");
            var sig = invoke.DecodeSignature(this, null);

            Emit(name, "delegate " + sig.ReturnType + " " + name + GenericParameters(type.GetGenericParameters())
                + "(" + string.Join(", ", sig.ParameterTypes) + ")", obsolete, where);

            return;
        }

        Emit(name, mods + kind + " " + name + GenericParameters(type.GetGenericParameters())
            + (supers.Count > 0 ? " : " + string.Join(", ", supers) : ""), obsolete, where);

        var accessors = new HashSet<MethodDefinitionHandle>();

        foreach (var ph in type.GetProperties())
        {
            var prop = _md.GetPropertyDefinition(ph);
            var acc = prop.GetAccessors();
            var parts = new List<string>();
            MethodDefinitionHandle first = default;

            foreach (var (m, word) in new[] { (acc.Getter, "get"), (acc.Setter, "set") })
            {
                if (m.IsNil)
                    continue;

                accessors.Add(m);

                var access = (int)(_md.GetMethodDefinition(m).Attributes & MethodAttributes.MemberAccessMask);

                if (Accessible(access, type))
                {
                    parts.Add((access == (int)MethodAttributes.Public ? "" : "protected ") + word + ";");
                    first = first.IsNil ? m : first;
                }
            }

            if (parts.Count == 0)
                continue;

            var sig = prop.DecodeSignature(this, null);
            var index = sig.ParameterTypes.Length > 0 ? "[" + string.Join(", ", sig.ParameterTypes) + "]" : "";

            Emit(name + "#" + _md.GetString(prop.Name) + index, sig.ReturnType + " " + _md.GetString(prop.Name) + index + " { "
                + string.Join(" ", parts) + " }", obsolete || Obsolete(prop.GetCustomAttributes()), MethodLocation(first, where));
        }

        foreach (var eh in type.GetEvents())
        {
            var ev = _md.GetEventDefinition(eh);
            var acc = ev.GetAccessors();

            accessors.Add(acc.Adder);
            accessors.Add(acc.Remover);

            if (!acc.Adder.IsNil && Accessible((int)(_md.GetMethodDefinition(acc.Adder).Attributes & MethodAttributes.MemberAccessMask), type))
            {
                Emit(name + "#" + _md.GetString(ev.Name), "event " + Name(ev.Type) + " " + _md.GetString(ev.Name),
                    obsolete || Obsolete(ev.GetCustomAttributes()), where);
            }
        }

        foreach (var mh in type.GetMethods())
        {
            var m = _md.GetMethodDefinition(mh);
            var access = (int)(m.Attributes & MethodAttributes.MemberAccessMask);

            if (accessors.Contains(mh) || !Accessible(access, type) || _md.GetString(m.Name) == ".cctor")
                continue;

            DescribeMethod(name, type, mh, m, obsolete, where);
        }

        foreach (var fh in type.GetFields())
        {
            var f = _md.GetFieldDefinition(fh);
            var access = (int)(f.Attributes & FieldAttributes.FieldAccessMask);

            if ((f.Attributes & FieldAttributes.SpecialName) != 0 || !Accessible(access, type))
                continue;

            var value = (f.Attributes & FieldAttributes.Literal) != 0 ? " = " + ConstantText(f.GetDefaultValue()) : "";
            var fieldMods = kind == "enum" ? "" : ((f.Attributes & FieldAttributes.Literal) != 0 ? "const "
                : ((f.Attributes & FieldAttributes.Static) != 0 ? "static " : "") + ((f.Attributes & FieldAttributes.InitOnly) != 0 ? "readonly " : ""));

            Emit(name + "#" + _md.GetString(f.Name), fieldMods + (kind == "enum" ? "" : f.DecodeSignature(this, null) + " ")
                + _md.GetString(f.Name) + value, obsolete || Obsolete(f.GetCustomAttributes()), where);
        }
    }

    /** */
    private void DescribeMethod(string type, TypeDefinition owner, MethodDefinitionHandle mh, MethodDefinition m, bool obsolete, string where)
    {
        var sig = m.DecodeSignature(this, null);
        var name = _md.GetString(m.Name);
        var attrs = m.Attributes;
        var iface = (owner.Attributes & TypeAttributes.Interface) != 0;
        var mods = (attrs & MethodAttributes.Static) != 0 ? "static "
            : iface ? ""
            : (attrs & MethodAttributes.Abstract) != 0 ? "abstract "
            : (attrs & MethodAttributes.Virtual) != 0 && (attrs & MethodAttributes.Final) != 0 ? "sealed override "
            : (attrs & MethodAttributes.Virtual) != 0 && (attrs & MethodAttributes.NewSlot) == 0 ? "override "
            : (attrs & MethodAttributes.Virtual) != 0 ? "virtual " : "";
        var parameters = m.GetParameters().Select(p => _md.GetParameter(p)).Where(p => p.SequenceNumber > 0)
            .ToDictionary(p => p.SequenceNumber - 1);
        var texts = new List<string>();

        for (var i = 0; i < sig.ParameterTypes.Length; i++)
        {
            var text = sig.ParameterTypes[i];

            if (parameters.TryGetValue(i, out var p))
            {
                if (Has(p.GetCustomAttributes(), "System", "ParamArrayAttribute"))
                    text = "params " + text;

                if ((p.Attributes & ParameterAttributes.HasDefault) != 0)
                    text += " = " + ConstantText(p.GetDefaultValue());
                else if ((p.Attributes & ParameterAttributes.Optional) != 0)
                    text += " = default";
            }

            texts.Add(text);
        }

        var generic = sig.GenericParameterCount > 0 ? "`" + sig.GenericParameterCount : "";
        var key = type + "#" + name + generic + "(" + string.Join(",", sig.ParameterTypes) + ")";

        var shown = name == ".ctor" ? Regex.Replace(_md.GetString(owner.Name), "`\\d+$", "") : sig.ReturnType + " " + name;

        Emit(key, mods + shown + GenericParameters(m.GetGenericParameters())
            + "(" + string.Join(", ", texts) + ")", obsolete || Obsolete(m.GetCustomAttributes()), MethodLocation(mh, where));
    }

    /// <summary>Returns generic parameters named by position, with their variance and constraints.</summary>
    private string GenericParameters(GenericParameterHandleCollection parameters)
    {
        if (parameters.Count == 0)
            return "";

        var texts = new List<string>();

        foreach (var h in parameters)
        {
            var p = _md.GetGenericParameter(h);
            var variance = p.Attributes & GenericParameterAttributes.VarianceMask;
            var constraints = p.GetConstraints().Select(c => Name(_md.GetGenericParameterConstraint(c).Type)).OrderBy(s => s).ToList();
            var special = p.Attributes & GenericParameterAttributes.SpecialConstraintMask;

            if ((special & GenericParameterAttributes.ReferenceTypeConstraint) != 0)
                constraints.Insert(0, "class");

            if ((special & GenericParameterAttributes.NotNullableValueTypeConstraint) != 0)
                constraints.Insert(0, "struct");

            if ((special & GenericParameterAttributes.DefaultConstructorConstraint) != 0)
                constraints.Add("new()");

            texts.Add((variance == GenericParameterAttributes.Covariant ? "out " : variance == GenericParameterAttributes.Contravariant ? "in " : "")
                + "#" + (p.Index + 1) + (constraints.Count > 0 ? " : " + string.Join(", ", constraints) : ""));
        }

        return "<" + string.Join(", ", texts) + ">";
    }

    /// <summary>Returns true if the attributes include [Obsolete].</summary>
    private bool Obsolete(CustomAttributeHandleCollection attributes) => Has(attributes, "System", "ObsoleteAttribute");

    /** */
    private bool Has(CustomAttributeHandleCollection attributes, string ns, string name)
    {
        foreach (var h in attributes)
        {
            var ctor = _md.GetCustomAttribute(h).Constructor;

            if (ctor.Kind != HandleKind.MemberReference)
                continue;

            var parent = _md.GetMemberReference((MemberReferenceHandle)ctor).Parent;

            if (parent.Kind == HandleKind.TypeReference)
            {
                var t = _md.GetTypeReference((TypeReferenceHandle)parent);

                if (_md.GetString(t.Namespace) == ns && _md.GetString(t.Name) == name)
                    return true;
            }
        }

        return false;
    }

    /// <summary>Returns a constant as C# writes it.</summary>
    private string ConstantText(ConstantHandle h)
    {
        if (h.IsNil)
            return "default";

        var c = _md.GetConstant(h);
        var r = _md.GetBlobReader(c.Value);

        switch (c.TypeCode)
        {
            case ConstantTypeCode.Boolean: return r.ReadBoolean() ? "true" : "false";
            case ConstantTypeCode.Char: return "'" + Escape(r.ReadChar().ToString()) + "'";
            case ConstantTypeCode.SByte: return r.ReadSByte().ToString();
            case ConstantTypeCode.Byte: return r.ReadByte().ToString();
            case ConstantTypeCode.Int16: return r.ReadInt16().ToString();
            case ConstantTypeCode.UInt16: return r.ReadUInt16().ToString();
            case ConstantTypeCode.Int32: return r.ReadInt32().ToString();
            case ConstantTypeCode.UInt32: return r.ReadUInt32().ToString();
            case ConstantTypeCode.Int64: return r.ReadInt64().ToString();
            case ConstantTypeCode.UInt64: return r.ReadUInt64().ToString();
            case ConstantTypeCode.Single: return r.ReadSingle().ToString("R", System.Globalization.CultureInfo.InvariantCulture);
            case ConstantTypeCode.Double: return r.ReadDouble().ToString("R", System.Globalization.CultureInfo.InvariantCulture);
            case ConstantTypeCode.String: return "\"" + Escape(r.ReadUTF16(r.Length)) + "\"";
            default: return "null";
        }
    }

    /** */
    private static string Escape(string s) =>
        s.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n").Replace("\r", "\\r").Replace("\t", "\\t");

    /// <summary>Returns the source file and line of a type, or the project directory if the file is not found.</summary>
    private string TypeLocation(TypeDefinitionHandle h)
    {
        var top = h;

        while (!_md.GetTypeDefinition(top).GetDeclaringType().IsNil)
            top = _md.GetTypeDefinition(top).GetDeclaringType();

        var simple = Regex.Replace(_md.GetString(_md.GetTypeDefinition(h).Name), "`\\d+$", "");
        var decl = new Regex("\\b(class|interface|struct|enum|delegate)\\s+([\\w.<>]+\\s+)?" + Regex.Escape(simple) + "\\b");
        var named = _files.GetValueOrDefault(Regex.Replace(_md.GetString(_md.GetTypeDefinition(top).Name), "`\\d+$", ""));

        // A type is usually declared in a file named after it; otherwise every file of the project is searched.
        foreach (var file in named != null ? new[] { named } : _files.Values.OrderBy(f => f).ToArray())
        {
            var lines = File.ReadAllLines(file);

            for (var i = 0; i < lines.Length; i++)
            {
                if (decl.IsMatch(lines[i]))
                    return Relative(file) + ":" + (i + 1);
            }
        }

        return named != null ? Relative(named) : "";
    }

    /// <summary>Returns the first source line of a method body, or the location of its type for a method without a body.</summary>
    private string MethodLocation(MethodDefinitionHandle h, string typeLocation)
    {
        if (h.IsNil)
            return typeLocation;

        foreach (var sp in _pdb.GetMethodDebugInformation(h.ToDebugInformationHandle()).GetSequencePoints())
        {
            if (!sp.IsHidden)
                return Relative(_pdb.GetString(_pdb.GetDocument(sp.Document).Name)) + ":" + sp.StartLine;
        }

        return typeLocation;
    }

    /** */
    private string Relative(string path) => Path.GetRelativePath(_root, path).Replace('\\', '/');

    /** */
    private void Emit(string key, string value, bool obsolete, string location)
    {
        var cols = new[] { Area, key, (obsolete ? "[Obsolete] " : "") + value, obsolete ? "deprecated" : "", location };

        if (cols.Any(c => c.Contains('\t') || c.Contains('\n') || c.Contains('\r')))
            throw new InvalidOperationException("Tab or line break in " + key);

        _out.WriteLine(string.Join("\t", cols));
        _count++;
    }

    /// <summary>Returns the name of a type definition, such as Apache.Ignite.Core.Cache.ICache`2 or Outer.Inner.</summary>
    private string TypeName(TypeDefinitionHandle h)
    {
        var t = _md.GetTypeDefinition(h);
        var name = _md.GetString(t.Name);

        if (!t.GetDeclaringType().IsNil)
            return TypeName(t.GetDeclaringType()) + "." + name;

        var ns = _md.GetString(t.Namespace);

        return ns.Length > 0 ? ns + "." + name : name;
    }

    /** */
    private string Name(EntityHandle h) => h.Kind switch
    {
        HandleKind.TypeDefinition => GetTypeFromDefinition(_md, (TypeDefinitionHandle)h, 0),
        HandleKind.TypeReference => GetTypeFromReference(_md, (TypeReferenceHandle)h, 0),
        HandleKind.TypeSpecification => GetTypeFromSpecification(_md, null, (TypeSpecificationHandle)h, 0),
        _ => h.Kind.ToString(),
    };

    /// <summary>Returns a type name without its generic arity, which the type arguments show.</summary>
    private static string Plain(string name) => Regex.Replace(name, "`\\d+", "");

    public string GetPrimitiveType(PrimitiveTypeCode code) => code switch
    {
        PrimitiveTypeCode.Boolean => "bool",
        PrimitiveTypeCode.Byte => "byte",
        PrimitiveTypeCode.SByte => "sbyte",
        PrimitiveTypeCode.Char => "char",
        PrimitiveTypeCode.Int16 => "short",
        PrimitiveTypeCode.UInt16 => "ushort",
        PrimitiveTypeCode.Int32 => "int",
        PrimitiveTypeCode.UInt32 => "uint",
        PrimitiveTypeCode.Int64 => "long",
        PrimitiveTypeCode.UInt64 => "ulong",
        PrimitiveTypeCode.Single => "float",
        PrimitiveTypeCode.Double => "double",
        PrimitiveTypeCode.String => "string",
        PrimitiveTypeCode.Object => "object",
        PrimitiveTypeCode.Void => "void",
        PrimitiveTypeCode.IntPtr => "System.IntPtr",
        PrimitiveTypeCode.UIntPtr => "System.UIntPtr",
        PrimitiveTypeCode.TypedReference => "System.TypedReference",
        _ => code.ToString(),
    };

    public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind) => TypeName(handle);

    public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind)
    {
        var t = reader.GetTypeReference(handle);
        var name = reader.GetString(t.Name);

        if (t.ResolutionScope.Kind == HandleKind.TypeReference)
            return GetTypeFromReference(reader, (TypeReferenceHandle)t.ResolutionScope, rawTypeKind) + "." + name;

        var ns = reader.GetString(t.Namespace);

        return ns.Length > 0 ? ns + "." + name : name;
    }

    public string GetTypeFromSpecification(MetadataReader reader, object context, TypeSpecificationHandle handle, byte rawTypeKind) =>
        reader.GetTypeSpecification(handle).DecodeSignature(this, context);

    public string GetSZArrayType(string elementType) => elementType + "[]";

    public string GetArrayType(string elementType, ArrayShape shape) => elementType + "[" + new string(',', shape.Rank - 1) + "]";

    public string GetByReferenceType(string elementType) => "ref " + elementType;

    public string GetPointerType(string elementType) => elementType + "*";

    public string GetPinnedType(string elementType) => elementType;

    public string GetGenericInstantiation(string genericType, ImmutableArray<string> typeArguments) =>
        Plain(genericType) + "<" + string.Join(", ", typeArguments) + ">";

    public string GetGenericTypeParameter(object genericContext, int index) => "#" + (index + 1);

    public string GetGenericMethodParameter(object genericContext, int index) => "#m" + (index + 1);

    public string GetModifiedType(string modifier, string unmodifiedType, bool isRequired) => unmodifiedType;

    public string GetFunctionPointerType(MethodSignature<string> signature) => "delegate*";
}
