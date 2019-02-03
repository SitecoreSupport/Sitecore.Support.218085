// © 2016 Sitecore Corporation A/S. All rights reserved.

using System;
using System.Data.SqlClient;
using System.Threading;
using System.Transactions;
using Sitecore.Diagnostics;

namespace Sitecore.Support.ExM.Framework.DataProviders
{
  /// <summary>
  /// A collection of utility methods related to SQL.
  /// </summary>
  internal static class SqlUtil
  {
    /// <summary>
    /// The error number that identifies an <see cref="SqlException"/> caused by a deadlock.
    /// </summary>
    private const int DeadlockExceptionNumber = 1205;

    /// <summary>
    /// Invokes an <paramref name="action"/>. In case the action fails due to an SQL
    /// deadlock a number of retries will be attempted before an exception is thrown.
    /// The action will be invoked inside a <see cref="TransactionScope"/>.
    /// </summary>
    /// <param name="action">The action to invoke.</param>
    /// <param name="maxNumberOfAttempts">The maximum number of attempt to invoke the action.</param>
    internal static void InvokeWithDeadlockRetry([NotNull] Action action, int maxNumberOfAttempts)
    {
      Assert.ArgumentNotNull(action, "action");
      Assert.ArgumentCondition(maxNumberOfAttempts > 0, "maxNumberOfAttempts", "The maximum number of attempts to invoke the action must be positive.");

      InvokeWithDeadlockRetry(
          () =>
          {
            action();
            return 0;
          },
          maxNumberOfAttempts);
    }

    /// <summary>
    /// Invokes a <paramref name="function"/> and returns the result. In case the function fails
    /// due to an SQL deadlock a number of retries will be attempted before an exception is thrown.
    /// The function will be invoked inside a <see cref="TransactionScope"/>.
    /// </summary>
    /// <typeparam name="TReturn">The return type of the <paramref name="function"/>.</typeparam>
    /// <param name="function">The function to execute.</param>
    /// <param name="maxNumberOfAttempts">The maximum number of attempt to invoke the action.</param>
    /// <returns>The value returned by the <paramref name="function"/>.</returns>
    internal static TReturn InvokeWithDeadlockRetry<TReturn>([NotNull] Func<TReturn> function, int maxNumberOfAttempts)
    {
      Assert.ArgumentNotNull(function, "function");
      Assert.ArgumentCondition(maxNumberOfAttempts > 0, "maxNumberOfAttempts", "The maximum number of attempts to invoke the action must be positive.");

      var attemptNum = 1;

      while (true)
      {
        try
        {
          using (var transaction = new TransactionScope())
          {
            var result = function();

            transaction.Complete();
            return result;
          }
        }
        catch (SqlException ex)
        {
          if (attemptNum >= maxNumberOfAttempts || ex.Number != DeadlockExceptionNumber)
          {
            throw;
          }
        }

        attemptNum++;

        Thread.Yield();
      }
    }
  }
}
