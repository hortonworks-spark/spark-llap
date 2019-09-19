/*

 */

package org.apache.spark.sql.catalyst.json

import java.io.Writer

import org.apache.spark.sql.types._

object JacksonGeneratorHelper {

  def createJacksonGenerator(
                              dataType: DataType,
                              writer: Writer,
                              options: JSONOptions = new JSONOptions( Map.empty, "UTC")): JacksonGenerator = {

    new JacksonGenerator(dataType, writer, options)

  }

}
