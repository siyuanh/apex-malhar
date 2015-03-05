/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.google.common.base.Preconditions;

@QRType(type=GenericSchemaResult.TYPE)
@ResultSerializerInfo(clazz=GenericSchemaSerializer.class)
public class GenericSchemaResult extends Result
{
  public static final String TYPE = "schemaData";

  private GenericSchema genericSchema;

  public GenericSchemaResult(SchemaQuery schemaQuery,
                      GenericSchema genericSchema)
  {
    super(schemaQuery);
    setGenericSchema(genericSchema);
  }

  private void setGenericSchema(GenericSchema genericSchema)
  {
    Preconditions.checkNotNull(genericSchema);
    this.genericSchema = genericSchema;
  }

  public GenericSchema getGenericSchema()
  {
    return genericSchema;
  }
}