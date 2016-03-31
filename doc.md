<div id="table-of-contents">
<h2>Table of Contents</h2>
<div id="text-table-of-contents">
<ul>
<li><a href="#orgheadline2">1. FDW DDL</a>
<ul>
<li><a href="#orgheadline1">1.1. SERVER OPTIONs</a></li>
</ul>
</li>
</ul>
</div>
</div>

# FDW DDL<a id="orgheadline2"></a>

## SERVER OPTIONs<a id="orgheadline1"></a>

The key-value pairs specified in the OPTIONS clause of the CREATE SERVER
command; e.g. the OPTION `host` is specified with a value '127.0.0.1' in
the example below:

    CREATE SERVER cass_serv FOREIGN DATA WRAPPER cstar_fdw
        OPTIONS (host '127.0.0.1');

The full list of the supported SERVER OPTIONs follows:

<table border="2" cellspacing="0" cellpadding="6" rules="groups" frame="hsides">


<colgroup>
<col  class="org-left" />

<col  class="org-left" />

<col  class="org-left" />

<col  class="org-left" />

<col  class="org-left" />
</colgroup>
<thead>
<tr>
<th scope="col" class="org-left">Key</th>
<th scope="col" class="org-left">Required</th>
<th scope="col" class="org-left">Description</th>
<th scope="col" class="org-left">Example Values</th>
<th scope="col" class="org-left">Default Value</th>
</tr>
</thead>

<tbody>
<tr class="tr-odd">
<td class="org-left">host</td>
<td class="org-left">Y</td>
<td class="org-left">The address(es) or hostname(s) of the Cassandra server(s)</td>
<td class="org-left">"127.0.0.1", "127.0.0.1,127.0.0.2", "server1.domain.com"</td>
<td class="org-left">N/A</td>
</tr>


<tr class="tr-even">
<td class="org-left">port</td>
<td class="org-left">N</td>
<td class="org-left">The port number of the Cassandra server(s)</td>
<td class="org-left">9042</td>
<td class="org-left">9042</td>
</tr>


<tr class="tr-odd">
<td class="org-left">protocol</td>
<td class="org-left">N</td>
<td class="org-left">The protocol version to use for connecting with the Cassandra server</td>
<td class="org-left">"4"</td>
<td class="org-left">"4" (for cpp-driver 2.3)</td>
</tr>
</tbody>
</table>
