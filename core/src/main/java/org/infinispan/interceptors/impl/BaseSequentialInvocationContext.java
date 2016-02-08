package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SequentialInvocationContext;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.interceptors.SequentialInterceptor;
import org.infinispan.util.Cons;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.NDC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * This base class implements the {@link org.infinispan.context.SequentialInvocationContext} methods.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public abstract class BaseSequentialInvocationContext
      implements InvocationContext, SequentialInvocationContext {
   private static final Log log = LogFactory.getLog(BaseSequentialInvocationContext.class);
   private static final boolean trace = log.isTraceEnabled();
   // Enable when debugging BaseSequentialInvocationContext itself
   private static final boolean EXTRA_LOGS = SecurityActions.getBooleanProperty("org.infinispan.debug.BaseSequentialInvocationContext");

   private static final CompletableFuture<Void> CONTINUE_INVOCATION = CompletableFuture.completedFuture(null);
   private static final int INVOKE_NEXT = 0;
   private static final int SHORT_CIRCUIT = 1;
   private static final int STOP_INVOCATION = 2;
   private static final int FORK_INVOCATION = 3;

   // The next interceptor to execute
   private Cons<SequentialInterceptor> nextInterceptor;
   // The next return handler to execute
   private Cons<SequentialInterceptor.ReturnHandler> nextReturnHandler;
   private CompletableFuture<Object> future;
   private int action;
   private Object actionValue;

   @Override
   public final CompletableFuture<Void> onReturn(SequentialInterceptor.ReturnHandler returnHandler) {
      nextReturnHandler = Cons.make(returnHandler, nextReturnHandler);
      return CONTINUE_INVOCATION;
   }

   @Override
   public final CompletableFuture<Void> continueInvocation() {
      return CONTINUE_INVOCATION;
   }

   @Override
   public final CompletableFuture<Void> shortCircuit(Object returnValue) {
      preActionCheck();
      action = SHORT_CIRCUIT;
      actionValue = returnValue;
      return CONTINUE_INVOCATION;
   }

   @Override
   public final CompletableFuture<Void> forkInvocation(VisitableCommand newCommand,
         SequentialInterceptor.ForkReturnHandler forkReturnHandler) {
      preActionCheck();
      Cons<SequentialInterceptor> localNode = this.nextInterceptor;
      if (localNode == null) {
         throw new IllegalStateException(
               "Cannot call shortCircuit or forkInvocation after all interceptors have executed");
      }
      this.action = FORK_INVOCATION;
      ForkInfo forkInfo = new ForkInfo(newCommand, forkReturnHandler);
      forkInfo.savedInterceptor = localNode;
      this.actionValue = forkInfo;
      return CONTINUE_INVOCATION;
   }

   private void preActionCheck() {
      if (action != INVOKE_NEXT) {
         throw new IllegalStateException(
               "An interceptor can call shortCircuit or forkInvocation at most once. The current action is " +
                     actionName(action));
      }
   }

   @Override
   public Object forkInvocationSync(VisitableCommand newCommand) throws Throwable {
      Cons<SequentialInterceptor> savedInterceptorNode = nextInterceptor;
      Cons<SequentialInterceptor.ReturnHandler> savedReturnHandler = nextReturnHandler;
      nextReturnHandler = Cons.empty();
      try {
         try {
            Object returnValue = doInvokeNextSync(newCommand);
            return invokeReturnHandlersSync(newCommand, returnValue, null);
         } catch (Throwable t) {
            return invokeReturnHandlersSync(newCommand, null, t);
         }
      } finally {
         // If an exception was thrown from an interceptor/return handler, nextInterceptor is not empty
         assert nextReturnHandler.isEmpty();
         nextInterceptor = savedInterceptorNode;
         nextReturnHandler = savedReturnHandler;
      }
   }

   private Object invokeReturnHandlersSync(VisitableCommand command, Object returnValue, Throwable throwable)
         throws Throwable {
      while (!nextReturnHandler.isEmpty()) {
         SequentialInterceptor.ReturnHandler current = nextReturnHandler.head();
         nextReturnHandler = nextReturnHandler.tail();

         try {
            returnValue = invokeReturnHandlerSync(current, command, returnValue, throwable);
            throwable = null;
         } catch (Throwable t) {
            throwable = t;
         }
      }
      if (throwable == null) {
         return returnValue;
      } else {
         throw throwable;
      }
   }

   final CompletableFuture<Object> invoke(VisitableCommand command,
         Cons<SequentialInterceptor> firstInterceptor) {
      future = new CompletableFuture<>();
      nextInterceptor = firstInterceptor;
      nextReturnHandler = Cons.empty();
      action = INVOKE_NEXT;
      invokeNextWithContext(command, null, null);
      return future;
   }

   private void invokeNextWithContext(VisitableCommand command, Object returnValue, Throwable throwable) {
      // Populate the NDC even if TRACE is not enabled for org.infinispan.context.impl
      Object lockOwner = getLockOwner();
      if (lockOwner != null) {
         NDC.push(lockOwner.toString());
      }
      try {
         invokeNext(command, returnValue, throwable);
      } finally {
         NDC.pop();
      }
   }

   private void invokeNext(VisitableCommand command, Object returnValue, Throwable throwable) {
      Cons<SequentialInterceptor> interceptorNode = this.nextInterceptor;
      while (true) {
         if (action == FORK_INVOCATION) {
            // forkInvocation start
            // Start invoking a new command with the next interceptor.
            // Save the current command and interceptor, and restore them when the forked command returns.
            ForkInfo forkInfo = (ForkInfo) this.actionValue;
            forkInfo.savedCommand = command;
            command = forkInfo.newCommand;
            nextReturnHandler = Cons.make(forkInfo, nextReturnHandler);
         } else if (action == STOP_INVOCATION) {
            // forkInvocationSync end
            action = INVOKE_NEXT;
            return;
         } else if (action == SHORT_CIRCUIT) {
            returnValue = actionValue;
            // Skip the remaining interceptors
            interceptorNode = Cons.empty();
            nextInterceptor = Cons.empty();
         }
         action = INVOKE_NEXT;
         if (!interceptorNode.isEmpty()) {
            SequentialInterceptor interceptor = interceptorNode.head();
            interceptorNode = interceptorNode.tail();
            nextInterceptor = interceptorNode;
            if (trace) {
               log.tracef("Executing interceptor %s with command %s", className(interceptor),
                     className(command));
            }
            try {
               CompletableFuture<Void> nextFuture = interceptor.visitCommand(this, command);
               if (nextFuture.isDone()) {
                  returnValue = nextFuture.getNow(null);
                  // continue
               } else {
                  // Some of the interceptor's processing is async
                  // The execution will continue when the interceptor finishes
                  if (EXTRA_LOGS && trace)
                     log.tracef("Interceptor %s continues asynchronously", interceptor);
                  final VisitableCommand finalCommand = command;
                  nextFuture.whenComplete(
                        (rv1, throwable1) -> invokeNextWithContext(finalCommand, rv1, throwable1));
                  return;
               }
            } catch (Throwable t) {
               throwable = t;
               if (t instanceof CompletionException) {
                  throwable = t.getCause();
               }
               if (trace)
                  log.tracef("Interceptor %s threw exception %s", className(interceptor), throwable);
               action = INVOKE_NEXT;
               // Skip the remaining interceptors
               interceptorNode = Cons.empty();
               nextInterceptor = Cons.empty();
            }
         } else if (!nextReturnHandler.isEmpty()) {
            // Interceptors are done, continue with the return handlers
            SequentialInterceptor.ReturnHandler returnHandler = nextReturnHandler.head();
            nextReturnHandler = nextReturnHandler.tail();
            if (trace)
               log.tracef("Executing return handler %s with return value/exception %s/%s", nextReturnHandler,
                     returnHandler, className(returnValue), throwable);
            try {
               CompletableFuture<Object> handlerFuture = returnHandler.handle(this, command, returnValue, throwable);
               if (handlerFuture != null) {
                  if (handlerFuture.isDone()) {
                     // The future is already completed.
                     // Update the return value and continue with the next return handler.
                     // If the future was a ForkInfo, we will continue with an interceptor instead.
                     returnValue = handlerFuture.getNow(returnValue);
                     throwable = null;
                     // In case a fork return handler changed it
                     interceptorNode = nextInterceptor;
                  } else {
                     // Continue the execution asynchronously
                     if (EXTRA_LOGS && trace)
                        log.tracef("Return handler %s continues asynchronously", returnHandler);
                     final VisitableCommand finalCommand1 = command;
                     handlerFuture.whenComplete(
                           (rv1, throwable1) -> invokeNextWithContext(finalCommand1, rv1, throwable1));
                     return;
                  }
               }
            } catch (Throwable t) {
               if (trace)
                  log.tracef("Return handler %s threw exception %s", className(returnHandler), t);
               // Reset the return value to avoid confusion
               returnValue = null;
               throwable = t;
               // In case this was a fork return handler and nextInterceptor got reset
               // Skip the remaining interceptors
               interceptorNode = Cons.empty();
               nextInterceptor = Cons.empty();
            }
         } else {
            // No more interceptors and no more return handlers. We are done!
            if (EXTRA_LOGS && trace)
               log.tracef("Command %s done with return value/exception %s/%s", command,
                     className(returnValue), throwable);
            completeFuture(future, returnValue, throwable);
            return;
         }
      }
   }

   @SuppressWarnings("unchecked")
   private CompletableFuture<Object> handleForkReturn(ForkInfo forkInfo, Object returnValue,
         Throwable throwable) throws Throwable {
      nextInterceptor = forkInfo.savedInterceptor;
      // We are abusing type erasure so that we can handle the future in invokeNext
      CompletableFuture handlerFuture = forkInfo.forkReturnHandler
            .handle(this, forkInfo.savedCommand, returnValue, throwable);
      return handlerFuture;
   }

   Object invokeSync(VisitableCommand command, Cons<SequentialInterceptor> firstInterceptor)
         throws Throwable {
      future = null;
      nextInterceptor = firstInterceptor;
      nextReturnHandler = Cons.empty();
      action = INVOKE_NEXT;

      return forkInvocationSync(command);
   }

   private Object doInvokeNextSync(VisitableCommand command)
         throws Throwable {
      SequentialInterceptor interceptor = nextInterceptor.head();
      nextInterceptor = nextInterceptor.tail();

      if (trace) {
         log.tracef("Invoking interceptor %s with command %s", className(interceptor), className(command));
      }
      try {
         CompletableFuture<Void> nextVisitFuture;
         // Simplify the execution for double-dispatch interceptors
         if (interceptor instanceof DDSequentialInterceptor) {
            nextVisitFuture = (CompletableFuture<Void>) command
                  .acceptVisitor(this, (DDSequentialInterceptor) interceptor);
         } else {
            nextVisitFuture = interceptor.visitCommand(this, command);
         }
         CompletableFutures.await(nextVisitFuture);
         return handleActionSync(command);
      } catch (Throwable t) {
         if (trace) log.tracef("Exception from interceptor: %s", t);
         // Unwrap the exception from CompletableFutures.await
         Throwable throwable = t instanceof ExecutionException ? t.getCause() : t instanceof CompletionException ? t.getCause() : t;
         throw throwable;
      }
   }

   private Object handleActionSync(VisitableCommand command)
         throws Throwable {
      if (action == INVOKE_NEXT) {
         // Continue with the next interceptor
         return doInvokeNextSync(command);
      } else if (action == SHORT_CIRCUIT) {
         // Skip the rest of the interceptors
         nextInterceptor = Cons.empty();
         action = INVOKE_NEXT;
         return actionValue;
      } else if (action == FORK_INVOCATION) {
         // Continue with the next interceptor, but with a new command
         ForkInfo forkInfo = (ForkInfo) actionValue;
         forkInfo.savedCommand = command;
         nextReturnHandler = Cons.make(forkInfo, nextReturnHandler);
         action = INVOKE_NEXT;
         actionValue = null;
         return doInvokeNextSync(forkInfo.newCommand);
      } else {
         throw new IllegalStateException("Illegal action type: " + action);
      }
   }

   private Object invokeReturnHandlerSync(SequentialInterceptor.ReturnHandler returnHandler,
         VisitableCommand command, Object returnValue, Throwable throwable) throws Throwable {
      if (returnHandler instanceof ForkInfo) {
         ForkInfo forkInfo = (ForkInfo) returnHandler;
         if (trace)
            log.tracef("Invoking fork return handler %s with return value/exception: %s/%s",
                  className(forkInfo.forkReturnHandler), className(returnValue), className(throwable));
         CompletableFuture handlerFuture = handleForkReturn(forkInfo, returnValue, throwable);
         if (!handlerFuture.isDone()) {
            CompletableFutures.await(handlerFuture);
         }
         return handleActionSync(command);
      } else {
         if (trace)
            log.tracef("Invoking return handler %s with return value/exception: %s/%s",
                  className(returnHandler), className(returnValue), className(throwable));
         CompletableFuture<Object> handlerFuture =
               returnHandler.handle(this, command, returnValue, throwable);
         if (handlerFuture != null) {
            return CompletableFutures.await(handlerFuture);
         }

         // A return value of null means we preserve the existing return value/exception
         if (throwable != null) {
            throw throwable;
         } else {
            return returnValue;
         }
      }
   }

   private static <T, E extends Throwable> void completeFuture(CompletableFuture<T> future, T returnValue,
         E exception) {
      if (exception == null) {
         future.complete(returnValue);
      } else {
         future.completeExceptionally(exception);
      }
   }

   private static String className(Object o) {
      if (o == null)
         return "null";

      String fullName = o.getClass().getName();
      return fullName.substring(fullName.lastIndexOf('.') + 1);
   }

   private static String actionName(int action) {
      switch (action) {
         case INVOKE_NEXT:
            return "INVOKE_NEXT";
         case SHORT_CIRCUIT:
            return "SHORT_CIRCUIT";
         case STOP_INVOCATION:
            return "STOP_INVOCATION";
         case FORK_INVOCATION:
            return "FORK_INVOCATION";
         default:
            return "Unknown action " + action;
      }
   }

   @Override
   public InvocationContext clone() {
      try {
         BaseSequentialInvocationContext clone = (BaseSequentialInvocationContext) super.clone();
         return clone;
      } catch (CloneNotSupportedException e) {
         throw new CacheException("Impossible", e);
      }
   }

   private static class ForkInfo implements SequentialInterceptor.ReturnHandler {
      final VisitableCommand newCommand;
      final SequentialInterceptor.ForkReturnHandler forkReturnHandler;
      Cons<SequentialInterceptor> savedInterceptor;
      VisitableCommand savedCommand;

      ForkInfo(VisitableCommand newCommand, SequentialInterceptor.ForkReturnHandler forkReturnHandler) {
         this.newCommand = newCommand;
         this.forkReturnHandler = forkReturnHandler;
      }

      @Override
      public String toString() {
         return "ForkInfo{" + newCommand.getClass().getSimpleName() + "}";
      }

      @Override
      public CompletableFuture<Object> handle(InvocationContext ctx, VisitableCommand command, Object rv,
            Throwable throwable) throws Throwable {
         return ((BaseSequentialInvocationContext) ctx).handleForkReturn(this, rv, throwable);
      }
   }
}
