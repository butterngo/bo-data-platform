using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace BO.PG.SourceConnector.Extensions;

public static class StringExtensions
{
	public static string ToSnakeCase(this string input)
	{
		if (string.IsNullOrEmpty(input)) { return input; }

		var startUnderscores = Regex.Match(input, @"^_+");
		return startUnderscores + Regex.Replace(input, @"([a-z0-9])([A-Z])", "$1_$2").ToLower();
	}

	public static string ToSha256Hash(this string rawData, int? length = null)
	{
		// Create a SHA256 instance
		using (SHA256 sha256Hash = SHA256.Create())
		{
			// Compute the hash
			byte[] bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(rawData));

			// Convert byte array to a string
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < bytes.Length; i++)
			{
				builder.Append(bytes[i].ToString("x2"));
			}
			
			if (!length.HasValue) 
			{
				return builder.ToString();
			}

			return builder.ToString().Substring(0, length.Value);
		}
	}
}
