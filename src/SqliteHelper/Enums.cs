using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqliteHelper
{ 
    /// <summary>
    /// Enumeration containing the supported WHERE clause operators.
    /// </summary>
    public enum Operators
    {
        /// <summary>
        /// Boolean AND.
        /// </summary>
        And,
        /// <summary>
        /// Boolean OR.
        /// </summary>
        Or,
        /// <summary>
        /// The two values are equal to one another.
        /// </summary>
        Equals,
        /// <summary>
        /// The two values are not equal to one another.
        /// </summary>
        NotEquals,
        /// <summary>
        /// The left value is contained within the right value (list).
        /// </summary>
        In,
        /// <summary>
        /// The left value is not contained within the right value (list).
        /// </summary>
        NotIn,
        /// <summary>
        /// The left value contains the right value.
        /// </summary>
        Contains,
        /// <summary>
        /// The left value does not contain the right value.
        /// </summary>
        ContainsNot,
        /// <summary>
        /// The left value is greater than the right value.
        /// </summary>
        GreaterThan,
        /// <summary>
        /// The left value is greater than or equal to the right value.
        /// </summary>
        GreaterThanOrEqualTo,
        /// <summary>
        /// The left value is less than the right value.
        /// </summary>
        LessThan,
        /// <summary>
        /// The left value is less than or equal to the right value.
        /// </summary>
        LessThanOrEqualTo,
        /// <summary>
        /// The left value is between two values supplied in the right value.
        /// </summary>
        Between,
        /// <summary>
        /// The left value is null.
        /// </summary>
        IsNull,
        /// <summary>
        /// The left value is not null.
        /// </summary>
        IsNotNull
    }

    /// <summary>
    /// Type of data contained in the column.  Refer to https://www.sqlite.org/datatype3.html.
    /// </summary>
    public enum DataType
    {
        /// <summary>
        /// Integer.
        /// </summary>
        Integer,
        /// <summary>
        /// Text.
        /// </summary>
        Text,
        /// <summary>
        /// Blob.
        /// </summary>
        Blob,
        /// <summary>
        /// Real.
        /// </summary>
        Real,
        /// <summary>
        /// Numeric.
        /// </summary>
        Numeric,
        /// <summary>
        /// Null.
        /// </summary>
        Null
    }
}