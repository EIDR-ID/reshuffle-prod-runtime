const { EIDRConnector } = require('reshuffle-eidr-connector')
const { HttpConnector } = require('reshuffle')
const { ReshuffleEnterprise } = require('reshuffle-runtime-enterprise')
const app = new ReshuffleEnterprise()

/** Connector definitions **/

// Connector: eidr
let EIDRConnector__eidr_7693dac6e776496daf33cc15eb77768c
try {
EIDRConnector__eidr_7693dac6e776496daf33cc15eb77768c = new EIDRConnector(app, {'userId':'', 'partyId':'', 'password':'', 'domain':''}, 'eidr')

}
catch (err) {
app.getLogger().error('Runtime EIDRConnector instantiation error. Review configuration for connector eidr', err)
}

// Connector: API
let HttpConnector__API_bdde19de936d49c6a266aae83766cdf6
try {
HttpConnector__API_bdde19de936d49c6a266aae83766cdf6 = new HttpConnector(app, {'authKey':'', 'authScript':''}, 'API')

}
catch (err) {
app.getLogger().error('Runtime HttpConnector instantiation error. Review configuration for connector API', err)
}

/** Scripts definitions **/

// Script: checkQueryLimits
const script_9c49905643254b15a4f5644fb1abb6d6 = async (app, size, type, idOnly) => {
  // assumes one of the legal resoltion types
  // the caller is repsonsible for checking
  console.log("checkQueryLimits")
  console.log(size);
  console.log(type);
  console.log(idOnly)
  const getSysInfo = app.getScript('getSysInfo');
  const sysinfo = await getSysInfo(app);

  if (idOnly){
    //console.log("idonly")
    return (size <= sysinfo.limits.idOnly )
  }
  else if (type.toLowerCase() === 'simple') {
    //console.log(sysinfo)
    return (size <= sysinfo.limits.simple)
  }
  else{
    // one of the other types
    //console.log("other")
    return (size <= sysinfo.limits.other)
  }
  
}



// Script: getValidatedTypes
const script_d341c1cf401f447388c965f4d896e4fe = // Get validated type information from an HTTP request.
//
// @param req HTTP request
//
// @return An Error if the request has invalid types, or an
//         object with the following fields:
//           type - request type
//           format - output format
//           redirect - boolean
//           mixed - mixed (camel) case type name
//           lower - lower case type name
//
async ({ req }) => {
  let types = getTypes(req);

  if (types.type) {
    const validatedType = validateType(types.type)
    //console.log('Validated type:', validatedType);
    if (!validatedType) {
      return new Error(`Invalid resolution type: ${types.type}`);
    }
    types = Object.assign({}, types, validatedType);
  }

  const FORMATS = ['json', 'tsv', 'xml'];
  if (types.format && !FORMATS.includes(types.format.toLowerCase())) {
     return new Error(`Invalid resolution format: ${types.format}`);
  }

  return types;

  // Get a resolution type and a format from either query parameters or the
  // Accept header, if possible. Wery params take precedence over Accept. If
  // the request looks like it came from a browser, ignore the accept header
  // since it will have generic but confusing things in it like
  // 'application/xml'.
  //
  // NOTE: an Accept header that isn't in the list of what we know about
  // returns type and format null and redirect false, leaving the caller to
  // decide what to do. This is perhaps too generous.
  //
  // @param type Type string
  //
  // @return Type, format and redirect flag
  //
  function getTypes(req) {
    const types = { type: null, format: null, redirect: false };

    // First try the query parameters. If there is anything in the query
    // parameters, don't need to check the 'Accept' header.
    types.type = req.query.type;
    types.format = req.query.format;
    if (types.type || types.format) {
      if (types.format && types.format.toLowerCase() === 'xml') {
        types.redirect = true;
      }
      return types;
    }

    // Check the user agent. If it's a browser, use the default type and
    // format already set up above. Don't do the fancy send to the UI that
    // the DOI Proxy does.
    //console.log('User-Agent:', req.header('User-Agent'));
    let ua = req.header('User-Agent').toLowerCase();
    if (typeof ua === 'string') {
      ua = [ua];
    } else if (!Array.isArray(ua)) {
      ua = [];
    }
    const browserString = [ // use lower case to save converting later
      'applewebkit',
      'chrome',
      'edge',
      'firefox',
      'gecko',
      'mozilla',
      'safari',
    ];
    if (browserString.some(b => ua.includes(b))) {
      return types;
    }

    const accept = req.header('Accept');
    //console.log('Accept:', accept);
    if (accept && accept.startsWith('application/vnd.eidr.')) {
      const stripped = accept.substr('application/vnd.eidr.'.length);
      const parts = stripped.split('+');
      types.type= parts[0];
      if (parts.length > 1) {
        types.format = parts[1];
      }
      types.redirect = (types.format ==='xml');
    }
    else if (accept === 'application/json') {
      // no resolution type
      types.format = 'json';
      types.redirect = false;
    }
    else if (accept === 'text/tab-separated-values'){
      // no resolution type
      types.format = 'tsv';
      types.redirect = false;
    }
    else if (accept === 'text/html' ||
      accept ==='application/xhtml+xml' ||
      accept === 'application/xml') {
      // don;t even try to sort things out - it will just get redirected
      types.redirect = true;
    }
    else if (accept === 'application/xml' || accept === 'text/xml'){
      types.redirect = true;
    }

    return types;
  }

  // Validate a resolution type.
  //
  // @param type Type string
  //
  // @return For valid type, a mixed case version (for query params) and
  //         a lower case (for MIME type). Otherwise null
  //
  function validateType(type) {
    const TYPES = {
      alternateids: 'AlternateIDs',
      doikernel: 'DOIKernel',
      full: 'Full',
      linkedalternateids: 'LinkedAlternateIDs',
      provenance: 'Provenance',
      selfdefined: 'SelfDefined',
      simple: 'Simple'
    };

    if (typeof type !== 'string') {
      return null;
    }
    for (const [lower, mixed] of Object.entries(TYPES)) {
      if (type.toLowerCase() === lower || type === mixed) {
        return { lower, mixed };
      }
    }
    return null;
  }
}

// Script: getSysInfo
const script_edb4ad96d7a2417aa916579ecf0585a1 = async (app) => {
  const eidr = app.getConnector('eidr');
  const deps = require(`${__dirname}/../package.json`).dependencies;

  const limits = {
    idOnly:150000,
    simple: 50000,
    other: 1000
  }

  const versions= {
    ...eidr.info(),
    ...deps,
    scripts: 'unknown',
  };

  return { limits, versions };
}

// Script: validateID
const script_44499d1f9508432486f1f4bfa66f15c9 = // Validate an EIDR content or other ID with the
// following formats:
//
//   10.5240/xxxx-xxxx-xxxx-xxxx-xxxx-y
//   10.5238/xxxx-xxxx
//   10.5237/xxxx-xxxx
//
// where every 'x' is a hexadecimal digit (0-F) and 'y' is
// any digit (0-9) or a capital letter (A-Z).
//
// @param ID EIDR ID string
//
// @return true for valid IDs, false otherwise
//
async ({ id }) => {
  const contentRe = /^10\.5240\/([0-9A-F]{4}-){5}[0-9A-Z]$/;
  const otherRe = /^10\.523[79]\/[0-9A-F]{4}-[0-9A-F]{4}$/;
  return contentRe.test(id) || otherRe.test(id);
}

// Script: Info
const script_c63ded14d69d46f2be3580530e41603c = async (event, app) => {
  const { req, res } = event;
  res.set({'eidr-version': app.getConnector('eidr').info().eidrApiVersion});
  const getSysInfo = app.getScript('getSysInfo');
  const info = await getSysInfo(app);
  return res.json(info);
}

// Script: tableParse
const script_2bcfef5c29d34e08a6a37df2ee44d742 = async (event) => {
  const { str, hasHeader, separator } = event
  const rows = str.split('\n');
  if (0 < rows.length && hasHeader) {
    rows.shift();
  }
  return rows.map((row) => row.split(separator));
}

// Script: buildRedirectResponse
const script_aaa3721960684a729189ae235c222ca7 = // Build an HTTP redirect response.
//
// @param req       HTTP request
//
// @param res       HTTP response
//
// @param id        EIDR ID
//
// @param types     Validated types structure as returned
//                  by getValidatedTypes
//
// @return HTTP redirect response
//
async ({ req, res, id, types }) => {
  console.log('Redirect types:', types);
  const accept = req.header('Accept');
  console.log('Redirect accept:', accept);

  if (!id && !req.query.format) {
    return res.set('Accept', accept).redirect(302, `https://doi.org/${id}`);
  }

  // Construct a call to the DOI Proxy with what we have.
  // Format should only ever be XML if we get here, but check just in case.
  if (types.format !== 'xml') {
    console.warn(`Redirect query-param doens't have XML? type: ${
      types.type} format: ${types.format} redirect: ${types.redirect}`);
  }
  const locatt = `?locatt=type:${types.mixed || 'Full' }`;
  console.log('Redirect locatt:', locatt);
  return res.redirect(302, `https://doi.org/${id}${locatt}`);
}

// Script: queryHelper
const script_d687e6f96b0c4c23818d2dd7b2b0f43c = // decides what kind of query to do (XML or JSON-based)
// calls the connector
// normalizes the results in the requestd format
// sorts the results if applicable
// returns the sorted results

async (event, app) => {
  const { req, res } = event;
  const validateID = app.getScript('validateID');
  const getValidatedTypes = app.getScript('getValidatedTypes');
  const getDefaultType = app.getScript('getDefaultType');
  const buildRedirectResponse = app.getScript('buildRedirectResponse');
  const resolveID = app.getScript('resolveID');
  const eidr =  app.getConnector('eidr');
  const normalize = app.getScript('normalize');
  const checkQueryLimits = app.getScript('checkQueryLimits');
  const bbPromise = require ('bluebird');

  const contentType = event.req.headers['content-type'];
  //console.log(contentType)
  
  const authorization = event.req.header('Authorization');
  let body;
  if (contentType === 'application/json') {
    body = event.req.body
  } else if (contentType === 'text/plain' || contentType === 'text/xml') {
    body = await new Promise((resolve, reject) => {
      const chunks = []
      event.req.on('data', chunk => {
        chunks.push(chunk.toString('utf8'))
      })
      event.req.on('end', () => {
        resolve(chunks.join(''))
      })
      event.req.on('error', (e) => {
        reject(e)
      })
    })
  } else {
    return event.res.status(400).send(`Unsupported content type: ${contentType}`)
  }
  //console.log(body)

  let types = await getValidatedTypes({ req });
  if (types instanceof Error) {
    console.log("types were an error")
    return res.status(400).send(types.message);
  }
  if (types.type === undefined) {
    //console.log(types);
    // if nothing's there. use a default
    //console.log('Resolution type not defined')
    types = await getDefaultType();
    //console.log(types)
  }

  if (types.redirect) {
    return res.status(400).send('XML Redirect not supported');
  }
  let idOnly = req.query.idOnly;
  //console.log(idOnly);
  
  let root = req.query.root;
  //console.log(root);
  
  const DEFAULTPAGESIZE = 
      (idOnly || types.type === 'simple') ? 2500 : 1000 ;
  let pageNumber;
  let pageSize;
  let results;
  if (req.query.pageNumber){
    pageNumber = parseInt(req.query.pageNumber);
    if (Number.isNaN(pageNumber) || pageNumber < 1) {
      return event.res.status(400).send(`Bad page number: ${req.query.pageNumber}`) 
    }
  }
  else {
      pageNumber=1;
  }
  //console.log("pageNumber: " + pageNumber);

  if (req.query.pageSize == 0){
    // explicit pageSize 0 means return entire set of unpaged results
    pageSize = 0;
  }
  else if (req.query.pageSize){
    // there's something in pageSize; use it if it's a number
    pageSize = parseInt(req.query.pageSize);
    if (Number.isNaN(pageSize) || pageSize < 0) {
      return event.res.status(400).send(`Bad page size: ${req.query.pageSize}`) 
    }
  }
  else {
    // not present; use the default
    //console.log("using default page size");
    pageSize = DEFAULTPAGESIZE;
  }
 //console.log("pageSize: " + pageSize)

  try {
    if (pageSize != 0) {
      // console.log("pagesize != 0")
      const psOK = await checkQueryLimits(app, pageSize, types.type, idOnly)
      // console.log(psOK);
      if (!psOK){
        const bad = idOnly ? 'idOnly' : types.type;
        return event.res.status(400).send(`pageSize ${pageSize} too large for type ${bad}`)
      }
      results = await eidr.query(
        body, 
        { idOnly: idOnly || types.type !=='simple', pageNumber, pageSize, root },
        authorization,
      );
      results.pageSize = pageSize;
      results.pageNumber = pageNumber;
      results.currentSize = results.results.length;
      // totalMatches comes back from the connector
      //console.log("connector query returned")
      //console.log(results.pageSize);
      //console.log(results);
    }
    else {
      //console.log("pageSize == 0")
      let cumulative = [];
      let intermediate;
      pageSize = DEFAULTPAGESIZE;
      pageNumber = 1;
      intermediate = await eidr.query(
        body,
        { idOnly: idOnly || types.type!== 'simple', pageNumber, pageSize, root },
        authorization,
      );
      const expected = intermediate.totalMatches;
      const fullOK = await checkQueryLimits(app, expected, types.type, idOnly)
      //console.log(fullOK)
      if (!fullOK){
        const bad = idOnly ? 'idOnly' : types.type;
        return event.res.status(400).send(`Full query result size ${expected} too large for type ${bad}`)
      }
      
      cumulative = cumulative.concat(intermediate.results)
      //console.log(`expected: ${expected}`)
      while(cumulative.length < expected) {
        console.log(`cumulative length: ${cumulative.length}`)
        pageNumber++;
        intermediate = await eidr.query(
          body,
          { idOnly: idOnly || types.type !== 'simple', pageNumber, pageSize, root },
          authorization,
        );
        // XXX whcih is better?
       // cumulative = [...cumulative, ...intermediate.results]
        cumulative=cumulative.concat(intermediate.results)
      }
      //console.log(`cumulative length: ${cumulative.length}`)
      results = {
        totalMatches:expected,
        pageSize: 0,
        pageNumber: 1,
        currentSize: cumulative.length,
        results: cumulative
      }
    }
  }
  catch(err) {
    console.log(err);
    throw err;
  }

  let normalized;
  if (idOnly) {
    //console.log("processing idonly")
    results.idOnly = true;
    normalized = results.results;
  }
  else if (types.type === 'simple') {
    //console.log("processing simple");
    normalized = await Promise.all(
       results.results.map(info => normalize({info}))
    );
  }
  else {
    //console.log("processing other")
    // turn array of IDs into array of EIDR metadata
    const ids = results.results;
    //console.log(ids);
    normalized = 
      await bbPromise.map(ids, 
              function(id) { 
                return resolveID({ id, types })
              },
              {concurrency:1500}
            )
  } 
  //console.log("done normalizing")

  //console.log("got results")
  results.results = normalized;
  //console.log("----")
  //console.log(results);
  return { results, types };
}

// Script: resolveID
const script_c85e6c85f0e4480fb31b07e7f2ea53bc = //
// @param id        EIDR ID
//
// @param types     Validated types structure as returned
//                  by getValidatedTypes
//
// @return EIDR info for the ID
//
async ({ id, types }) => {
  app.getConnector('eidr');
  const eidr = app.getConnector('eidr');
  const normalize = app.getScript('normalize');
  const info = await eidr.resolve(id, types.mixed || 'Full');
  return normalize({ info });
}

// Script: tableStringify
const script_108dfe6c53a947e3a6e050aa57660c2c = async (event) => {
  const { table, header, separator } = event

  const hd = header || (0 < table.length ? table.shift() : undefined);
  if (!hd || hd.length === 0) {
    return ''
  }
  const rows = [hd.join(separator)];

  let r = 1;
  for (const row of table) {
    if (row.length !== hd.length) {
      throw new Error(`Row ${r} has ${row.length} columns instead of ${hd.length}`);
    }
    rows.push(row.join(separator));
    r += 1;
  }

  return rows.join('\n');
}

// Script: normalize
const script_7593e0bc639f4ef783900dd0495ad7ef = //
// Recursively clean up nodes in the information object
// returned from the EIDR API. The object properties are
// traversed and nested objects are treated resusively.
//
// @param inode      EIDR information object or a nested
//                   object
//
// @param parentName (optional) name of parent node (used
//                   for handling arrays)
//
// @return cleaned up information object
//
async ({ info }) => {

  const forceArray = [
    'Actor',
    'AlternateName',
    'AlternateNumber',
    'AlternateID',
    'AlternateResourceName',
    'AssociatedOrg',
    'CountryOfOrigin',
    'Details',
    'Director',
    'EditClass',
    'Element',
    'Entry',
    'EpisodeClass',
    'MadeForRegion',
    'MetadataAuthority',
    'OriginalLanguage',
    'SeasonClass',
    'VersionLanguage',
    'LinkedAlternateID',
    'URL',
    'AlternatePartyName',
    'AllowedRoles',
    'AlternateServiceName',
    'OtherAffiliation',
    'Identifier',
    'URI',
    'LinkedCration',
    'Party-Service',
  ];
  
  // The value on the right is the default property when forcing the object
  // ex: 'original': 'sampleProp', and the original object has {original: 'hi'}
  // after parsing, you get  {original: {sampleProp: 'hi'}}
  const forceObj = {
    'Description': 'value',
    'referentCreation.identifier[].type': 'value',
    'referentCreation.linkedCreation[].identifier.type': 'value',
    'referentCreation.principalAgent[].identifier.type': 'value'
  }
  
  return _normalize(info);
  function _normalize(inode, parentName='') {

    // Non-object values end the recursion immediately
    if (typeof inode !== 'object') {
      return inode;
    }

    const onode = {};

    function set(name, value, fullSelfName) {
      const nm = name.startsWith('md:') ? name.substr(3) : name;
      if(forceArray.includes(nm) && !Array.isArray(value)) {
        onode[nm] = [value];
      } else if(forceObj[fullSelfName] && typeof value !== 'object') {
        onode[nm] = {
          [forceObj[fullSelfName]]: value
        };
      } else {
        onode[nm] = value;
      }
    }

    // Recursively map the cleaned up fields of the input node
    // onto a newly created output node
    Object.entries(inode).map(([name, value]) => {
      
      const selfName =  (parentName ? parentName + '.' : '') + name + (Array.isArray(value) ? '[]' : '');
      // Map every array element into an boject and use the
      // original propety name in case the child was a string
      // value instead of a full node.
      if (Array.isArray(value)) {
        set(name, value.map(e => _normalize(e, selfName)), selfName);
      }

      // Add attributes as object element. At some point in the code, attributes are prefixed with '$'.
      // This will cause issues with Python, so we need to change '$' to '_' for all attribute names
      // to avoid conflit with other child nodes.
      else if (name === '$') {
        Object.entries(value).map(([an, av]) => {
          const nm = an.startsWith('xsi:') ? an.substr(4) : an;
          onode['_' + nm] = av;
        });
      }

      // Recursively handle child nodes.
      else if (typeof value === 'object') {
        set(name, _normalize(value, selfName), selfName);
      }

      // If a node has attributes and a direct string value (rather
      // than a child node) then use the parent node's name as
      // a proprty name for that value.
      else if (name === '_') {
        onode.value = value;
      }

      // Store value
      else {
        set(name, value, selfName);
      }
    });

    return onode;
  }
}

// Script: sortResults
const script_5f69cad3062747cc9c19362d420b4648 = async (app, records, sort, order) => {
    const sorter = app.getScript('sorter');
    compare = await sorter(sort, order? order : 'asc')
    if (compare){ 
      const sorted = [...records].sort(compare)
      return sorted
    }
    return records;
}

// Script: shallowFields
const script_c0e2c37b707441ddbb0ef94f16bb3e88 = async () => {

  // Define the fields that will be populated in the flat object. Each
  // property in the "fields" object below will be translated into one
  // or more properties of the flat information object. The value for
  // each property defines the path into the nested EIDR information
  // object.
  //
  // Regular fields are accessed via a dot notation, as in 'ID' or
  // 'ResourceName.ResourceName'. Attributes are similar but have an underscore '_'
  // prefix to their name, as in 'ResourceName._lang'.
  //
  // Arrays are handled by adding '[]' into the path, where the array
  // is. Property names should enbed the '#' sign, to be replaced with
  // a one-based index into the array.

  return [
// Row_ID generated in tableize
    ['EIDR_ID', 'ID'],
    ['StructuralType', 'StructuralType'],
    ['Mode', 'Mode'],
    ['ReferentType', 'ReferentType'],
    ['ResourceName', 'ResourceName.value'],
    ['ResourceName@lang', 'ResourceName._lang'],
    ['ResourceName@class', 'ResourceName._titleClass'],
    ['ResourceName@systemGenerated', 'ResourceName._systemGenerated'],
    ['AltResourceName-#', 'AlternateResourceName[].value'],
    ['AltResourceName-#@lang', 'AlternateResourceName[]._lang'],
    ['AltResourceName-#@class', 'AlternateResourceName[]._titleClass'],
    ['OriginalLanguage-#', 'OriginalLanguage[].value'],
    ['OriginalLanguage-#@mode', 'OriginalLanguage[]._mode'],
    ['OriginalLanguage-#@type', 'OriginalLanguage[]._type'],
    ['VersionLanguage-#', 'VersionLanguage[].value'],
    ['VersionLanguage-#@mode', 'VersionLanguage[]._mode'],
    ['VersionLanguage-#@type', 'VersionLanguage[]._type'],
    ['AssociatedOrg-#', 'AssociatedOrg[].DisplayName'],
// Nested array for Associated Org | Alternate Names
    ['AssociatedOrg-#_AlternateName-#', 'AssociatedOrg[].AlternateName[]'],
    ['AssociatedOrg-#@role', 'AssociatedOrg[]._role'],
    ['AssociatedOrg-#@organizationID', 'AssociatedOrg[]._organizationID'],
    ['AssociatedOrg-#@idType', 'AssociatedOrg[]._idType'],
    ['ReleaseDate', 'ReleaseDate'],
    ['CountryOfOrigin-#', 'CountryOfOrigin[]'],
    ['PublicationStatus', 'Status'],
    ['ApproxLength', 'ApproximateLength'],
    ['Director-#', 'Credits.Director[].DisplayName'],
    ['Actor-#', 'Credits.Actor[].DisplayName'],
    
    // Use an AlernateID "flag" for special handling to create dedicated Alt ID output columns
    ['AlternateID', 'AlternateID'],
    // ['AlternateID-#', 'AlternateID[].value'],
    // ['AlternateID-#@type', 'AlternateID[]._type'],
    // ['AlternateID-#@domain', 'AlternateID[]._domain'],
    // ['AlternateID-#@relation', 'AlternateID[]._relation'],

    ['Registrant', 'Administrators.Registrant'],
    ['MetadataAuthority-#', 'Administrators.MetadataAuthority[]'],
    ['RegistrantExtra', 'RegistrantExtra'],
    ['Description', 'Description.value'],
    ['Description@lang', 'Description._lang'],
// Series
    ['Series_EndDate', 'ExtraObjectMetadata.SeriesInfo.EndDate'],
    ['Series_Class', 'ExtraObjectMetadata.SeriesInfo.SeriesClass'],
    ['Series_NumberRequired', 'ExtraObjectMetadata.SeriesInfo.NumberRequired'],
    ['Series_DateRequired', 'ExtraObjectMetadata.SeriesInfo.DateRequired'],
    ['Series_OriginalTitleRequired', 'ExtraObjectMetadata.SeriesInfo.OriginalTitleRequired'],
// Season
    ['Season_Parent', 'ExtraObjectMetadata.SeasonInfo.Parent'],
    ['Season_EndDate', 'ExtraObjectMetadata.SeasonInfo.EndDate'],
    ['Season_Class-#', 'ExtraObjectMetadata.SeasonInfo.SeasonClass[]'],
    ['Season_NumberRequired', 'ExtraObjectMetadata.SeasonInfo.NumberRequired'],
    ['Season_DateRequired', 'ExtraObjectMetadata.SeasonInfo.DateRequired'],
    ['Season_OriginalTitleRequired', 'ExtraObjectMetadata.SeasonInfo.OriginalTitleRequired'],
    ['Season_SequenceNumber', 'ExtraObjectMetadata.SeasonInfo.SequenceNumber'],
// Episode
    ['Episode_Parent', 'ExtraObjectMetadata.EpisodeInfo.Parent'],
    ['Episode_DistributionNumber', 'ExtraObjectMetadata.EpisodeInfo.SequenceInfo.DistributionNumber.value'],
    ['Episode_DistributionNumber@domain', 'ExtraObjectMetadata.EpisodeInfo.SequenceInfo.DistributionNumber._domain'],
    ['Episode_HouseSequence', 'ExtraObjectMetadata.EpisodeInfo.SequenceInfo.HouseSequence'],
    ['Episode_HouseSequence@domain', 'ExtraObjectMetadata.EpisodeInfo.SequenceInfo.HouseSequence._domain'],
    ['Episode_AlternateNum-#', 'ExtraObjectMetadata.EpisodeInfo.SequenceInfo.AlternateNumer[]'],
    ['Episode_AlternateNum-#@domain', 'ExtraObjectMetadata.EpisodeInfo.SequenceInfo.AlternateNumber[]._domain'],
    ['Episode_Class-#', 'ExtraObjectMetadata.EpisodeInfo.EpisodeClass[]'],
    ['Episode_TimeSlot', 'ExtraObjectMetadata.EpisodeInfo.TimeSlot'],
// Edit
    ['Edit_Parent', 'ExtraObjectMetadata.EditInfo.Parent'],
    ['Edit_Use', 'ExtraObjectMetadata.EditInfo.EditUse'],
    ['Edit_Class-#', 'ExtraObjectMetadata.EditInfo.EditClass[]'],
    ['Edit_MadeForRegion-#', 'ExtraObjectMetadata.EditInfo.MadeForRegion[]'],
    ['Edit_Details-#', 'ExtraObjectMetadata.EditInfo.Details[]'],
    ['Edit_Details-#@domain', 'ExtraObjectMetadata.EditInfo.Details[]._domain'],
    ['Edit_ColorType', 'ExtraObjectMetadata.EditInfo.ColorType'],
    ['Edit_ThreeD', 'ExtraObjectMetadata.EditInfo.ThreeD'],
// Clip
    ['Clip_Parent', 'ExtraObjectMetadata.ClipInfo.Parent'],
    ['Clip_ComponentsMode', 'ExtraObjectMetadata.ClipInfo.ComponentsMode'],
    ['Clip_Start', 'ExtraObjectMetadata.ClipInfo.Start'],
    ['Clip_Duration', 'ExtraObjectMetadata.ClipInfo.Duration'],
// Manifestations
    ['Manif_Parent', 'ExtraObjectMetadata.ManifestationInfo.Parent'],
    ['Manif_Class-#', 'ExtraObjectMetadata.ManifestationInfo.ManifestationClass[]'],
    ['Manif_MadeForRegion-#', 'ExtraObjectMetadata.ManifestationInfo.MadeForRegion[]'],
    ['Manif_Details-#', 'ExtraObjectMetadata.ManifestationInfo.Details[]'],
    ['Manif_Details-#@domain', 'ExtraObjectMetadata.ManifestationInfo.Details[]._domain'],
    ['Manif_HasDigital', 'ExtraObjectMetadata.ManifestationInfo.Digital?'],
// Compilation
    ['Compilation_Class', 'ExtraObjectMetadata.CompilationInfo.CompilationClass'],
    ['Compilation_Class@hasOtherInclusions', 'ExtraObjectMetadata.CompilationInfo.CompilationClass._hasOtherInclusions'],
    ['Compilation_EntryID-#', 'ExtraObjectMetadata.CompilationInfo.Entry[].ContentID'],
    ['Compilation_DisplayName-#', 'ExtraObjectMetadata.CompilationInfo.Entry[].DiplayName'],
    ['Compilation_DisplayName-#@lang', 'ExtraObjectMetadata.CompilationInfo.Entry[].DiplayName._language'],
    ['Compilation_EntryNumber-#', 'ExtraObjectMetadata.CompilationInfo.Entry[].EntryNumber'],
    ['Compilation_EntryClass-#', 'ExtraObjectMetadata.CompilationInfo.Entry[].EntryClass'],
// Composite
    ['Composite_Class', 'ExtraObjectMetadata.CompositeInfo.CompositeClass'],
    ['Composite_Element-#', 'ExtraObjectMetadata.CompositenInfo.Element[].ID'],
    ['Composite_Element-#OtherID', 'ExtraObjectMetadata.CompositeInfo.Element[].OtherID'],
    ['Composite_Element-#OtherID@relation', 'ExtraObjectMetadata.CompositeInfo.Element[].OtherID._relation'],
    ['Composite_Element-#OtherID@type', 'ExtraObjectMetadata.CompositeInfo.Element[].OtherID._type'],
    ['Composite_Element-#ComponentsMode', 'ExtraObjectMetadata.CompositenInfo.Element[].ComponentsMode'],
    ['Composite_Element-#SourceStart', 'ExtraObjectMetadata.CompositenInfo.Element[].SourceStart'],
    ['Composite_Element-#SourceDuration', 'ExtraObjectMetadata.CompositenInfo.Element[].SourceDuration'],
    ['Composite_Element-#DestStart', 'ExtraObjectMetadata.CompositenInfo.Element[].DestStart'],
    ['Composite_Element-#DestDuration', 'ExtraObjectMetadata.CompositenInfo.Element[].DestDuration'],
    ['Composite_Element-#Description', 'ExtraObjectMetadata.CompositenInfo.Element[].Description'],
// Lightweight Relationships
// Promotion
    ['Promotion-#ID', 'ExtraObjectMetadata.PromotionInfo[].ID'],
    ['Promotion-#Class', 'ExtraObjectMetadata.PromotionInfo[].PromotionClass'],
// Supplemental Content
    ['SupplementalContent-#ID', 'ExtraObjectMetadata.SupplementalContentInfo[].ID'],
    ['SupplementalContent-#Class', 'ExtraObjectMetadata.SupplementalContentInfo[].SupplementalContentClass'],
// Alternate Content
    ['AlternateContent-#ID', 'ExtraObjectMetadata.AlternateContent[].ID'],
    ['AlternateContent-#Class', 'ExtraObjectMetadata.AlternateContent[].AlternateContentClass'],
// Packaging
    ['Packaging-#ID', 'ExtraObjectMetadata.PackagingInfo[].ID'],
    ['Packaging-#Class', 'ExtraObjectMetadata.PackagingInfo[].PackagingClass'],
// Provenance (unique values only)
    ['Provenance_IssueNumber', 'IssueNumber'],
    ['CreatedBy', 'CreatedBy'],
    ['CreationDate', 'CreationDate'],
    ['LastModifiedBy', 'LastModifiedBy'],
    ['LastModificationDate', 'LastModificationDate'],
    ['PublicationDate', 'PublicationDate'],
// Alternate IDs
    //All values are defined above
// Linked Alternate IDs
    ['LinkedAlternateID-#', 'LinkedAlternateID[].AlternateID[].value'],
    ['LinkedAlternateID-#@type', 'LinkedAlternateID[].AlternateID[]._type'],
    ['LinkedAlternateID-#@domain', 'LinkedAlternateID[].AlternateID[]._domain'],
    ['LinkedAlternateID-#@relation', 'LinkedAlternateID[].AlternateID[]._relation'],
    ['LinkedAlternateID-#_URL-#', 'LinkedAlternateID[].URL[].value'],
    ['LinkedAlternateID-#_URL-#@type', 'LinkedAlternateID[].URL[]._type'],
// Party
    //ID is defined above
    ['PartyName', 'PartyName.DisplayName'],
    ['AlternatePartyName-#', 'AlternatePartyName[]'],
    ['ContactName', 'ContactInfo.Name'],
    ['ContactEmail', 'ContactInfo.PrimaryEmail'],
    ['Active', 'Active'],
    ['AllowedRoles-#', 'AllowedRoles[]'],
// Video Service
    //ID is defined above
    ['ServiceName', 'ServiceName.DisplayName'],
    ['ServiceName@abbreviation', 'ServiceName._abbreviation'],
    ['AlternateServiceName-#', 'AlternateServiceName[].value'],
    //@abbreviaion is optional.
    ['AlternateServiceName-#@abbreviation', 'AlternateServiceName[]._abbreviation'],
    //Alt ID is defined above
    //Description is defined above
    ['Service_Parent', 'Parent'],
    ['OtherAffiliation-#', 'OtherAffiliation[]'],
    //Active is defined above
    ['Format', 'Format'],
    ['PrimaryTimeZone', 'PrimaryTimeZone'],
    ['Region', 'Region'],
    ['PrimaryAudioLanguage', 'PrimaryAudioLanguage'],
    ['DeliveryModel', 'DeliveryModel'],
// DOI Kernel
    ['ReferentDOIName', 'referentDoiName'],
    ['PrimaryReferentType', 'primaryReferentType'],
    ['RegistrationAgencyDOIName', 'registrationAgencyDoiName'],
    ['IssueDate', 'issueDate'],
    ['DOI_IssueNumber', 'issueNumber'],
    ['Name', 'referentCreation.name.value'],
    ['Name@primaryLanguage', 'referentCreation.name._primaryLanguage'],
    ['Name@type', 'referentCreation.name.type'],
    ['Identifier-#', 'referentCreation.identifier[].nonUriValue'],
    ['Identifier-#_URI-#', 'referentCreation.identifier[].uri[].value'],
    ['Identifier-#_URI-#@returnType', 'referentCreation.identifier[].uri[]._returnType'],
    ['Identifier-#_URI-#@validNamespace', 'referentCreation.identifier[].uri[]._validNamespace'],
    ['Identifier-#_Type', 'referentCreation.identifier[].type.value'],
    ['Identifier-#_Type_Namespace', 'referentCreation.identifier[].type._validNamespace'],
    ['DOI_StructuralType', 'referentCreation.structuralType'],
    ['DOI_Mode-#', 'referentCreation.mode[]'],
    ['Character-#', 'referentCreation.character[]'],
    ['Type','referentCreation.type'],
    ['PrincipalAgent-#','referentCreation.principalAgent[].name.value'],
    ['PrincipalAgent-#_Type','referentCreation.principalAgent[].name.type'],
    ['PrincipalAgent-#_Value','referentCreation.principalAgent[].identifier.value'],
    ['PrincipalAgent-#_ValueType','referentCreation.principalAgent[].identifier.type.value'],
    ['PrincipalAgent-#_Role','referentCreation.principalAgent[].role'],
    ['LinkedCreation-#_Identifier-#','referentCreation.linkedCreation[].identifier[].nonUriValue'],
    ['LinkedCreation-#_Identifier-#_URI-#','referentCreation.linkedCreation[].identifier[].uri[].value'],
    ['LinkedCreation-#_Identifier-#_URI#@returnType','referentCreation.linkedCreation[].identifier[].uri[]._returnType'],
    ['LinkedCreation-#_Identifier-#_Type','referentCreation.linkedCreation[].identifier[].type.value'],
    ['LinkedCreation-#_Identifier-#_Type@namespace','referentCreation.linkedCreation[].identifier[].type._validNamespace'],
    ['LinkedCreation-#_Role','referentCreation.linkedCreation[].linkedCreationRole'],
    ['Party-Service-#', 'referentParty.name[].value'],
    ['Party-Service-#@type', 'referentParty.name[].type'],
    ['Party-ServiceID', 'referentParty.identifier'],
    ['Party-ServiceStructuralType', 'referentParty.structuralType'],
    ['Party-ServiceRole', 'referentParty.associatedRole'],
    ['Party-ServiceTerritory', 'referentParty.associatedTerritory'],
    ['Party-ServiceLinkedParty', 'referentParty.linkedParty'],
    
 ];
 
}


// Script: tsv
const script_8c9cf40cbc204476963d23d969224a34 = async (event, app) => {
  const resolveID = app.getScript('resolveID');
  const shallowFields = app.getScript('shallowFields');
  const shallow = app.getScript('shallow');
  const tabelize = app.getScript('tabelize');
  const tableStringify = app.getScript('tableStringify');

  const fields = await shallowFields();
  const shallows = await Promise.all(event.infos.map(nested => shallow({ nested, fields })));
  const table = await tabelize({
    shallows,
    fields,
    pageNumber: event.pageNumber,
    pageSize: event.pageSize,
  });

  return tableStringify({ table, separator: '\t' });
}

// Script: tabelize
const script_110622f3ccda4a8d965656861a1f082e = async (event) => {

  const rowOffset = event.pageNumber && event.pageSize ?
    (event.pageNumber - 1) * event.pageSize : 0;

  const hm = createHeaderMap(event.shallows);
  const nl = createdNestedHeaderList(event.fields, hm);
  const hl = flattenHeaderList(event.shallows, hm, nl);
  return populateTableByHeaders(event.shallows, hl, rowOffset);

  //
  // Create a map of all the keys that have at least one value
  // in one of the shallow objects
  //
  function createHeaderMap(shallows) {
    const headerMap = {};
    for (const shallow of shallows) {
      for (const [key, value] of Object.entries(shallow)) {
        if (value !== undefined) {
          const nm = key.split(/-\d/g).filter(s => 0 < s.length).join('-');
          headerMap[key] = headerMap[nm] = nm;
        }
      }
    }
    return headerMap;
  }

  // Create a list of headers that have corresponding values in the
  // header map. Arrays appear as array with '-#' placeholders and
  // may be nested.
  //
  function createdNestedHeaderList(fields, headerMap) {
    const arrays = [];
    const arrayNames = [];
    const nestedList = ['Row_ID'];

    function startArray(name) {
      arrays.push([]);
      arrayNames.push(name);
    }

    function endArray() {
      arrayNames.pop();
      addHeaders(arrays.pop());
    }

    function addHeaders(...properties) {
      const target = arrays.length ? arrays[arrays.length - 1] : nestedList;
      target.push(...properties);
    }


    // SL: 
    function addAltIDHeaders() {
      masterKey = [];
      event.shallows.forEach((shallow, i) => {
        keys = Object.keys(shallow).filter((key) => {
          return key.indexOf('AlternateID_') === 0 && masterKey.indexOf(key) === -1;
        });
        masterKey.push(...keys);
      })
  
      return masterKey
        .sort((a, b) => {
        if((b && !a) || b.toLowerCase() > a.toLowerCase()) {
          return -1;
        }
        if((a && !b) || a.toLowerCase() > b.toLowerCase()) {
          return 1;
        }
  
        return 0;
      })
    }
    // SL: End
    
    for (const [property, path] of fields) {
      
      //Forces alphabetical sorting for Alternate IDs
      if(property === 'AlternateID') {
        nestedList.push(...addAltIDHeaders())
        continue;
      }

      const split = property.split('-#');
      const nestingLevel = split.length - 1;
      const arrayName = split[split.length - 2];
      if (arrays.length < nestingLevel) {
        startArray(arrayName);
      }
      else if (nestingLevel < arrays.length) {
        while (nestingLevel < arrays.length) {
          endArray();
        }
      }
      else if (arrays.length && arrayNames[arrays.length - 1] !== arrayName) {
        endArray();
        startArray(arrayName);
      }
      if (headerMap[split.filter(s => 0 < s.length).join('-')]) {
        addHeaders(property);
      }
    }

    while (0 < arrays.length) {
      endArray();
    }

    return nestedList;
  }

  //
  // Converted nested headers list into a flat list, rendering all array
  // indexes according to data availability.
  //
  function flattenHeaderList(shallows, headerMap, nestedList) {
    const headerList = [];
    addHeaders(nestedList);
    return headerList;

    function addHeaders(nl, indexes = []) {
      for (const header of nl) {
        if (Array.isArray(header)) {
          addArray(header)
        } else {
          headerList.push(header)
        }
      }
    }

    function addArray(array, indexes = []) {
      let tempArr = array;
      while (Array.isArray(tempArr)) {
        tempArr = tempArr[0];
      }
      if (!tempArr) {
        return;
      }
  
      const nm = replaceWithIndexes(array[0], indexes, true); // header[0] might be an array
      const num = `Num_${nm}`;
      if(headerList.indexOf(num) === -1) {
        headerList.push(num);
      } else {
        return;
      }
  
      const count = Math.max(
        ...shallows.map(shallow => shallow[num] || 0)
      );
      
      for (let i = 1; i <= count; i++) {
        const idx = [...indexes, i];
        for (let j = 0; j < array.length; j++) {
          const hd = array[j];
          const newHd = [];
          if (Array.isArray(hd)) {
            for(let k = 0; k < hd.length; k++) {
              if(typeof hd[k] === 'string') {
                newHd[k] = replaceWithIndexes(hd[k], idx);
              } else {
                newHd[k] = hd[k];
              }
            }
            addArray(newHd, [])
          } else {
            const key = replaceWithIndexes(hd, idx);
            if (headerMap[key]) {
              headerList.push(key);
            }
          }
        }
      }
    }

    function replaceWithIndexes(str, indexes, trim = false) {
      const max = trim ? indexes.length : Number.MAX_SAFE_INTEGER;
      const alt = trim ? '' : '-#';
      const array = str
        .split('-#')
        .filter((s, i) => i <= max)
        .map((s, i) => ([s, i < indexes.length ? `-${indexes[i]}` : alt]))
        .flat()
      return array.slice(0, array.length - 1).join('');
    }
  }

  //
  // Create a table with the specified headers and each row representing
  // the data from one of the shallows.
  //
  function populateTableByHeaders(shallows, headers, rowOffset = 0) {
    const table = [ headers ];
    for (let r = 0; r < event.shallows.length; r++) {
      const rowId = r + 1 + rowOffset;
      const sh = shallows[r];
      const row = headers.map(key => key === 'Row_ID' ? rowId : sh[key]);
      table.push(row);
    }
    return table;
  }

}

// Script: Query
const script_1fe9472b38044f93acc1361e0ff8f4f4 = async (event, app) => {
  const { req, res } = event;

  const logRequestStats = app.getScript('logRequestStats');
  function logStats(params) {
    return logRequestStats({
      req,
      params: { type: 'Query', ...params },
    });
  }

  console.log('Query POST')
  const tsv = app.getScript('tsv');
  const queryHelper = app.getScript('queryHelper');
  const sortResults = app.getScript('sortResults');
  res.set({ 'eidr-version': app.getConnector('eidr').info().eidrApiVersion });

  try {
    let { results, types } = await queryHelper(event, app);
    if (!results) {
      await logStats({ statusCode: 500, errorMessage: 'queryHelper error' });
      return
    }
    console.log("queryHelper got results");

    await logStats({ statusCode: 200 });

    if (req.query && req.query.sort){ 
      const sorted = await sortResults(app, results.results, req.query.sort, req.query.order)
      results.results = sorted;
    }
    console.log("query sorting done")
  
    if (results.idOnly && types.format === "tsv") {
      const list =["EIDR_ID"].concat(results.results)
      return res.send(list.join("\n"))
    }
  
    return types.format === 'tsv' ? 
      res
        .set({'Content-Type': 'text/tab-separated-values'})
        .send(await tsv({
          infos: results.results,
          pageNumber: results.pageNumber,
          pageSize: results.pageSize,
        }, app)) : 
      res.json(results);

  } catch (e) {
    if (e.message.startsWith('EIDRConnector')) {
      console.error(e);
      await logStats({ statusCode: e.status, errorMessage: e.details });
      return event.res.status(e.status).send(e.details);
    } else {
      console.error(e);
      await logStats({ statusCode: 500, errorMessage: 'Connector error' });
      return event.res.status(500).send('Connector error');
    }
  }
}

// Script: shallow
const script_0f3d9ccd2de440d98c827c2b3f5ddd8b = //
// Convert EIDR resource info into shallow (un-nested) object.
// Not all of the original information is converted.
//
// @param event EIDR information object
//
// @return shallow object
//
async (event) => {
  const fields = Object.fromEntries(event.fields);

  // Traverse the fields object, and add a flatenned property to the
  // flat object for each or its (property, path) pair.
  const shallow = {};
  for (const [property, path] of Object.entries(fields)) {
    flatten(shallow, property, event.nested, path)
  }
  return shallow;
  
  // Special handling for Alt ID column consolidation and alphabetical sorting
  function flattenAltID(tgt, altIDList) {
    currentItemCount = {};
    
    function createAltIDProp(name, src) {
      if(currentItemCount[name]) {
          currentItemCount[name] ++;
        } else {
          currentItemCount[name] = 1;
        }
        
        // 01/01/2021 Added type
        tgt['AlternateID_' + name + '-' + currentItemCount[name] + '@type'] = src._type === 'Proprietary'? src._type : name;
        // 01/01/2021 End
        
        tgt['AlternateID_' + name + '-' + currentItemCount[name]] = src.value;
        if(src._relation) {
          tgt['AlternateID_' + name + '-' + currentItemCount[name] + '@relation'] = src._relation;
        }
    }
    
    altIDList.forEach((src, i) => {
      if(src._type === 'Proprietary') {
        createAltIDProp(src._domain, src, true);
      } else {
        createAltIDProp(src._type, src);
      }
    })
  }
  
  function replaceWithIndexes(str, indexes, trim = false) {
    const max = trim ? indexes.length : Number.MAX_SAFE_INTEGER;
    const alt = trim ? '' : '-#';
    const array = str
      .split('-#')
      .filter((s, i) => i <= max)
      .map((s, i) => ([s, i < indexes.length ? `-${indexes[i]}` : alt]))
      .flat()
    return array.slice(0, array.length - 1).join('');
  }
  
  //
  // Recusively extract a path from a nested object and populate a flat
  // object.
  //
  // @param tgt      target flat object (passed through recursion steps)
  // @param property name of target flat object property (passed through
  //                 recursion steps)
  // @param src      source information object (recurse through nested
  //                 child structure of the original source object)
  // @param path     path into source object (trimmed during recursion)
  // @param indexes  array of indexes into potentially nested arrays
  //
  function flatten(tgt, property, src, path, indexes=[]) {
    if (!src) {
      return;
    }

    // break down path into the next step (which is the property name in
    // the current source object) and the rest of the path
    const next = path.substr(0, path.indexOf('.'));
    const rest = path.substr(path.indexOf('.') + 1);

    // No next means this is the last step in the recursion over path
    // elements and rest is the last property name

    //If you encounter the Alternate ID flag, then swith to Alternate ID-specific flattening, else process as normal
    if(property === 'AlternateID' && src[property] && src[property].length) {
      flattenAltID(tgt, src[property]);
    }
    
    else if (!next && rest.endsWith('?')) {
      const nm = indexes.reduce((s, i) => s.replace('#', i), property);
      tgt[nm] = src[rest.substr(0, rest.length - 1)] ? 'TRUE' : '';
    }

    else if (!next && !rest.endsWith('[]')) {
      const nm = indexes.reduce((s, i) => s.replace('#', i), property);
      tgt[nm] = src[rest];
    }

    // Flatten a string array
    else if (!next && rest.endsWith('[]')) {
      const prop = rest.substr(0, rest.length - 2)

      const array = src[prop];
      if (Array.isArray(array) && array && array.length) {
        array.forEach((item, i) => {
          const nm = [...indexes, i + 1].reduce((s, i) => s.replace('#', i), property);
          tgt[nm] = item;
          tgt['Num_' + nm.substr(0, nm.length - 2)] = array.length;
        })
      }
    }

    // This is an object array, flatten every element separately
    else if (next.endsWith('[]')) {
      const object = src[next.substr(0, next.length - 2)];
      let array = Array.isArray(object) ? object : [object];
      array.map((element, index) => {
        flatten(tgt, property, element, rest, [...indexes, index + 1]);
      });
      array = array.filter(item => item !== undefined && item !== null);
      
      if (!property.includes('@')) {
        if(indexes && indexes.length) {
          const propArray = property.split('-');
          propArray.pop();
          const name = 'Num_' + replaceWithIndexes(propArray.join('-'), indexes);
          tgt[name] = array.length;
        } else {
          tgt['Num_' +  property.split('-')[0]] = array.length;
        }
      }
    }

    // Recurse into the next element of the path
    else {
      flatten(tgt, property, src[next], rest, indexes);
    }
  }
}


// Script: logRequestStats
const script_8b9d42f29da045339f53d832823d9ecb = async (event, app) => {
  const AWS = require('aws-sdk');
  const elasticsearch = require('@elastic/elasticsearch');
  const createAwsElasticsearchConnector = require('aws-elasticsearch-connector');

  AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION,
  });

  const client = new elasticsearch.Client({
    ...createAwsElasticsearchConnector(AWS.config),
    node: process.env.ELASTICSEARCH_NODE_URL,
  });

  const indexName =
    process.env.ELASTICSEARCH_INDEX_NAME + '-' +
    (new Date()).toLocaleDateString('ja', { year: 'numeric', month: '2-digit' }).replace('/', '-')

  client.index({
    index: indexName,
    type: 'doc',
    body: {
      name: 'request',
      timestamp: (new Date()).toISOString(),
      ip: event.req.headers['x-forwarded-for'] || event.req.connection.remoteAddress,
      ...(event.params || {}),
    },
  });
}

// Below are the standard Registry log fields (in order) -- awaiting further notes to verify specific contents of each
//
// action -- Resolve; ResolveMultiple; Query?
// dedupe_status -- N/A
// doi -- EIDR ID for Resolve -- some sort of delimited list for Resolve-Multiple? 
// dupes -- N/A
// hash
// high_score_dupes -- N/A
// linenum (long)
// logger
// loglevel
// logsrc
// msg
// partyid -- From the user's credentials (or the default once we get that set in an environment variable)
// thread
// timestamp (date) : DONE -- we're working without a template, but we've converted the date into the correct string format
// userid -- From the user's credentials (or the default once we get that set in an environment variable)
// ip : DONE -- NOT part of the Registry log (it seems), so added to the end of the log line


// Script: getDefaultType
const script_ac71ffd8a9e146bdaed7f227e7facc18 = // Get a default resolution type
//
// @param none
//
// @return return an object with the following fields
//           type: 'Full'
//           format: 'json'
//           redirect: false
//           mixed: 'Full'
//           lower: 'full'
//
async () => {
  const defaultType = {
    type: 'Full',
    format: 'json',
    redirect: false,
    mixed: 'Full',
    lower: 'full'
  }
  return defaultType;
}

// Script: ResolveOneID
const script_187674a836264ead89ef32dfd1f8c173 = //
// Resolve one EIDR resource ID.
//
// Request path should include a single an EIDR ID as described
// in "validateID".
//
// Response body is a JSON encoded object with the information
// about the identified media resource.
//
async (event, app) => {
  const { req, res } = event;

  const logRequestStats = app.getScript('logRequestStats');
  function logStats(params) {
    return logRequestStats({
      req,
      params: { type: 'ResolveOneID', ...params },
    });
  }

  const validateID = app.getScript('validateID');
  const getValidatedTypes = app.getScript('getValidatedTypes');
  const buildRedirectResponse = app.getScript('buildRedirectResponse');
  const resolveID = app.getScript('resolveID');
  const tsv = app.getScript('tsv');
  res.set({'eidr-version': app.getConnector('eidr').info().eidrApiVersion});

  const suffix = req.params.suffix;
  const prefix = req.params.prefix;
//  console.log(`${prefix}/${suffix}`)

  const id = `${prefix}/${suffix}`
  if (!await validateID({ id })) {
    const errorMessage = `Invalid ID: ${prefix}/${suffix}`;
    await logStats({ statusCode: 400, errorMessage });
    return res.status(400).send(errorMessage);
  }

  const types = await getValidatedTypes({ req });
  if (types instanceof Error) {
    await logStats({ statusCode: 400, errorMessage: types.message });
    return res.status(400).send(types.message);
  }
  /*
  if (types.format === 'tsv') {
    return new Error('TSV not supported for single ID resolution');
  }
  */

  if (types.redirect) {
    return buildRedirectResponse({ req, res, id, types });
  }

  try {
    const info = await resolveID({ id, types });
    await logStats({ statusCode: 200 });
    return types.format === 'tsv' ? 
      res.set({'Content-Type': 'text/tab-separated-values'}).send(await tsv({infos:[info]}, app)) : 
      res.json(info);
  } 
  catch (e) {
    if (e.message.startsWith('EIDRConnector')) {
      console.error(e);
      await logStats({ statusCode: e.status, errorMessage: e.details });
      return event.res.status(e.status).send(e.details);
    } else {
      console.error(e);
      await logStats({ statusCode: 500, errorMessage: 'Connector error' });
      return event.res.status(500).send('Connector error');
    }
  }
}

// Script: sorter
const script_732731b8b1fa4a069e751d8f9576a939 = // sort is the standard sort type form a query parameter
// order is 'asc' or 'desc'
// returns function that takes two normalized EIDR records
// or null if the sort types doens;t exist 
 
async (sort, order = 'asc') =>{
  //SORTKEYS has
  // field: from query sort parameter, name of top-level EIDR field,
  // accessfn: returns a string or number after dealing with
  // conversions, finding things inside of objects, etc
  const SORTKEYS = {
    title: {
      field: 'ResourceName', 
      accessfn: function(val) {return val.ResourceName}
    }, 
    date: {
      field: 'ReleaseDate',
      accessfn: function(val) {return val}
    }, 
    length:{
      field:  'ApproximateLength',
      accessfn: function(val) {
        // parse it (no y/m/d or sign in EIDR durations)
        var iso8601DurationRegex = /PT(?:([.,\d]+)H)?(?:([.,\d]+)M)?(?:([.,\d]+)S)?/;
        const components = val.match(iso8601DurationRegex);
        const H = components[1] ? parseInt(components[1]): 0;
        const M = components[2] ?parseInt(components[2]): 0;
        const S = components[3] ?parseInt(components[3]): 0;
        // convert to seconds
        return (H*60*60)+(M*60)+S
      }
    },
    coo: {
      field: 'CountryOfOrigin',
      accessfn: function(val) { return val }
    },
    struct: {
      field: 'StructuralType',
      accessfn: function(val) { return val }
    },
    reftype: {
      field: 'ReferentType',
      accessfn: function(val) { return val }
    },
    lang: {
      field: 'OriginalLanguage',
      accessfn: function(val) { return val.value }
   }
  };
  const action = SORTKEYS[sort];
  if (!action) {
    return null;
  }
  const key = action.field
  const accessfn = action.accessfn;
  return function sortNormalizedRecords(a, b) {
    // nothing to compare
    if (!a.hasOwnProperty(key) || !b.hasOwnProperty(key)) {
      console.log("----not found:" + key)
      return 0;
    }
    let varA = getSortValue(a[key], accessfn)
    let varB = getSortValue(b[key], accessfn)
    //console.log(`varA: ${varA} varB: ${varB}`)
    let comparison = 0;
    if (varA > varB) {
      comparison = 1;
    } else if (varA < varB) {
      comparison = -1;
    }
    return (
      (order === 'desc') ? (comparison * -1) : comparison
    );
    
    // deal with properties that are arrays
    // value is a property of an eidr reoslution format
    // accessfn get the right string out of that value
    function getSortValue(val, accessfn){
      if (Array.isArray(val)){
        const extracted = val.map((item) => accessfn(item))
        // sort it and concatenate it
        const flatval = [...extracted].sort()
        return flatval.join();
      }
      else {
        return accessfn(val)
      }
    }
  }
}


// Script: ResolveMultipleIDs
const script_b80a6646a6df41179c63d113b96572ab = //
// Resolve multiple EIDR resource ID.
//
// Request body should be a JSON object with a single property
// names 'ids' whose value is a list of EIDR ID strings as
// described in "validateID".
//
// Response body is a JSON encoded object with the information
// about the identified media resource.
//
async (event, app) => {
  const { req, res } = event;

  const logRequestStats = app.getScript('logRequestStats');
  function logStats(params) {
    return logRequestStats({
      req,
      params: { type: 'ResolveMultipleID', ...params },
    });
  }

//console.log('resolve multiple');
  const validateID = app.getScript('validateID');
  const getValidatedTypes = app.getScript('getValidatedTypes');
  const buildRedirectResponse = app.getScript('buildRedirectResponse');
  const resolveID = app.getScript('resolveID');
  const tsv = app.getScript('tsv');
  const ids = req.body.ids;
  res.set({'eidr-version': app.getConnector('eidr').info().eidrApiVersion});

//console.log(ids);
  if (!Array.isArray(ids)) {
    const errorMessage = `Invalid 'ids' array is request body`;
    await logStats({ statusCode: 400, errorMessage });
    return res.status(400).send(errorMessage);
  }
  for (const id of ids) {
    if (!await validateID({ id })) {
      const errorMessage = `Invalid ID: ${id}`;
      await logStats({ statusCode: 400, errorMessage });
      return res.status(400).send(errorMessage);
    }
  }
  console.log('validated')

  const types = await getValidatedTypes({ req });
  if (types instanceof Error) {
    await logStats({ statusCode: 400, errorMessage: types.message });
    return res.status(400).send(types.message);
  }

  if (types.redirect) {
    const errorMessage = 'XML Redirect not supported';
    await logStats({ statusCode: 400, errorMessage });
    return res.status(400).send(errorMessage);
  }

  try {
    const infos = await Promise.all(
      ids.map(id => resolveID({ id, types }))
    );
    await logStats({ statusCode: 200 });
    return types.format === 'tsv' ? 
      res.set({'Content-Type': 'text/tab-separated-values'}).send(await tsv({infos}, app)) : 
      res.json(infos);
  } 
  catch (e) {
    if (e.message.startsWith('EIDRConnector')) {
      console.log(e);
      await logStats({ statusCode: e.status, errorMessage: e.details });
      return event.res.status(e.status).send(e.details);
    } else {
        console.error(e);
        await logStats({ statusCode: 500, errorMessage: 'Connector error' });
        return event.res.status(500).send('Connector error');
    }
  }
}

/** Event handling definitions **/

try {
HttpConnector__API_bdde19de936d49c6a266aae83766cdf6 && HttpConnector__API_bdde19de936d49c6a266aae83766cdf6.on(
{'method':'GET', 'path':'info'}
, script_c63ded14d69d46f2be3580530e41603c, '0873a03d-24bc-4f0e-8594-a50b916b6e48')

}
catch (err) {
app.getLogger().error('Runtime Http event creation error. Review configuration for event Info', err)
}
try {
HttpConnector__API_bdde19de936d49c6a266aae83766cdf6 && HttpConnector__API_bdde19de936d49c6a266aae83766cdf6.on(
{'path':'query', 'method':'POST'}
, script_1fe9472b38044f93acc1361e0ff8f4f4, 'dbd41091-23c8-400a-b60d-2ee0a5948e96')

}
catch (err) {
app.getLogger().error('Runtime Http event creation error. Review configuration for event Query', err)
}
try {
HttpConnector__API_bdde19de936d49c6a266aae83766cdf6 && HttpConnector__API_bdde19de936d49c6a266aae83766cdf6.on(
{'method':'GET', 'path':'resolve/:prefix/:suffix'}
, script_187674a836264ead89ef32dfd1f8c173, 'ddab991f-9083-45be-918a-5fbdc395fc90')

}
catch (err) {
app.getLogger().error('Runtime Http event creation error. Review configuration for event Resolve One ID', err)
}
try {
HttpConnector__API_bdde19de936d49c6a266aae83766cdf6 && HttpConnector__API_bdde19de936d49c6a266aae83766cdf6.on(
{'method':'POST', 'path':'resolve'}
, script_b80a6646a6df41179c63d113b96572ab, 'b6e1ebf6-5365-472a-9d4b-e79fba969af6')

}
catch (err) {
app.getLogger().error('Runtime Http event creation error. Review configuration for event Resolve Multiple IDs', err)
}

/** Handler registrations **/

app.registerHandler(script_9c49905643254b15a4f5644fb1abb6d6, '9c499056-4325-4b15-a4f5-644fb1abb6d6', 'checkQueryLimits')
app.registerHandler(script_d341c1cf401f447388c965f4d896e4fe, 'd341c1cf-401f-4473-88c9-65f4d896e4fe', 'getValidatedTypes')
app.registerHandler(script_edb4ad96d7a2417aa916579ecf0585a1, 'edb4ad96-d7a2-417a-a916-579ecf0585a1', 'getSysInfo')
app.registerHandler(script_44499d1f9508432486f1f4bfa66f15c9, '44499d1f-9508-4324-86f1-f4bfa66f15c9', 'validateID')
app.registerHandler(script_c63ded14d69d46f2be3580530e41603c, 'c63ded14-d69d-46f2-be35-80530e41603c', 'Info')
app.registerHandler(script_2bcfef5c29d34e08a6a37df2ee44d742, '2bcfef5c-29d3-4e08-a6a3-7df2ee44d742', 'tableParse')
app.registerHandler(script_aaa3721960684a729189ae235c222ca7, 'aaa37219-6068-4a72-9189-ae235c222ca7', 'buildRedirectResponse')
app.registerHandler(script_d687e6f96b0c4c23818d2dd7b2b0f43c, 'd687e6f9-6b0c-4c23-818d-2dd7b2b0f43c', 'queryHelper')
app.registerHandler(script_c85e6c85f0e4480fb31b07e7f2ea53bc, 'c85e6c85-f0e4-480f-b31b-07e7f2ea53bc', 'resolveID')
app.registerHandler(script_108dfe6c53a947e3a6e050aa57660c2c, '108dfe6c-53a9-47e3-a6e0-50aa57660c2c', 'tableStringify')
app.registerHandler(script_7593e0bc639f4ef783900dd0495ad7ef, '7593e0bc-639f-4ef7-8390-0dd0495ad7ef', 'normalize')
app.registerHandler(script_5f69cad3062747cc9c19362d420b4648, '5f69cad3-0627-47cc-9c19-362d420b4648', 'sortResults')
app.registerHandler(script_c0e2c37b707441ddbb0ef94f16bb3e88, 'c0e2c37b-7074-41dd-bb0e-f94f16bb3e88', 'shallowFields')
app.registerHandler(script_8c9cf40cbc204476963d23d969224a34, '8c9cf40c-bc20-4476-963d-23d969224a34', 'tsv')
app.registerHandler(script_110622f3ccda4a8d965656861a1f082e, '110622f3-ccda-4a8d-9656-56861a1f082e', 'tabelize')
app.registerHandler(script_1fe9472b38044f93acc1361e0ff8f4f4, '1fe9472b-3804-4f93-acc1-361e0ff8f4f4', 'Query')
app.registerHandler(script_0f3d9ccd2de440d98c827c2b3f5ddd8b, '0f3d9ccd-2de4-40d9-8c82-7c2b3f5ddd8b', 'shallow')
app.registerHandler(script_8b9d42f29da045339f53d832823d9ecb, '8b9d42f2-9da0-4533-9f53-d832823d9ecb', 'logRequestStats')
app.registerHandler(script_ac71ffd8a9e146bdaed7f227e7facc18, 'ac71ffd8-a9e1-46bd-aed7-f227e7facc18', 'getDefaultType')
app.registerHandler(script_187674a836264ead89ef32dfd1f8c173, '187674a8-3626-4ead-89ef-32dfd1f8c173', 'ResolveOneID')
app.registerHandler(script_732731b8b1fa4a069e751d8f9576a939, '732731b8-b1fa-4a06-9e75-1d8f9576a939', 'sorter')
app.registerHandler(script_b80a6646a6df41179c63d113b96572ab, 'b80a6646-a6df-4117-9c63-d113b96572ab', 'ResolveMultipleIDs')
app.start()
