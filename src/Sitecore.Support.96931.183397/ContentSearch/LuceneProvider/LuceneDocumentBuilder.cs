using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Web;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.ComputedFields;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Data.LanguageFallback;

namespace Sitecore.Support.ContentSearch.LuceneProvider
{
    public class LuceneDocumentBuilder : Sitecore.ContentSearch.LuceneProvider.LuceneDocumentBuilder
    {
        public LuceneDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
        {
        }

        public override void AddItemFields()
        {
            try
            {
                VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");
                this.Indexable.LoadAllFields();
                if (this.IsParallel)
                {
                    ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
                    this.ParallelForeachProxy.ForEach<IIndexableDataField>(this.Indexable.Fields, this.ParallelOptions, delegate (IIndexableDataField f) {
                        try
                        {
                            this.CheckAndAddField(this.Indexable, f);
                        }
                        catch (Exception exception)
                        {
                            exceptions.Enqueue(exception);
                        }
                    });
                    if (exceptions.Count > 0)
                    {
                        throw new AggregateException(exceptions);
                    }
                }
                else
                {
                    foreach (IIndexableDataField field in this.Indexable.Fields)
                    {
                        this.CheckAndAddField(this.Indexable, field);
                    }
                }
            }
            finally
            {
                VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
            }
        }

        private void CheckAndAddField(IIndexable indexable, IIndexableDataField field)
        {
            string name = field.Name;
            if ((this.IsTemplate && this.Options.HasExcludedTemplateFields) && (this.Options.ExcludedTemplateFields.Contains(name) || this.Options.ExcludedTemplateFields.Contains(field.Id.ToString())))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was excluded.");
            }
            else if ((this.IsMedia && this.Options.HasExcludedMediaFields) && this.Options.ExcludedMediaFields.Contains(field.Name))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Media field was excluded.");
            }
            else if (this.Options.ExcludedFields.Contains(field.Id.ToString()) || this.Options.ExcludedFields.Contains(name))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was excluded.");
            }
            else
            {
                try
                {
                    LanguageFallbackFieldSwitcher switcher;
                    if (this.Options.IndexAllFields)
                    {
                        using (switcher = new LanguageFallbackFieldSwitcher(new bool?(this.Index.EnableFieldLanguageFallback)))
                        {
                            this.AddField(field);
                            return;
                        }
                    }
                    if (this.Options.IncludedFields.Contains(name) || this.Options.IncludedFields.Contains(field.Id.ToString()))
                    {
                        using (switcher = new LanguageFallbackFieldSwitcher(new bool?(this.Index.EnableFieldLanguageFallback)))
                        {
                            this.AddField(field);
                            return;
                        }
                    }
                    VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was not included.");
                }
                catch (Exception exception)
                {
                    if (this.Settings.StopOnCrawlFieldError())
                    {
                        throw;
                    }
                    CrawlingLog.Log.Fatal(string.Format("Could not add field {1} : {2} for indexable {0}", indexable.UniqueId, field.Id, field.Name), exception);
                }
            }
        }



        private static readonly MethodInfo AddComputedIndexFieldMethodInfo;
        static LuceneDocumentBuilder()
        {
            AddComputedIndexFieldMethodInfo = typeof(Sitecore.ContentSearch.LuceneProvider.LuceneDocumentBuilder).GetMethod("AddComputedIndexField", BindingFlags.Instance | BindingFlags.NonPublic);
        }

        protected override void AddComputedIndexFieldsInParallel()
        {
            ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
            var needEnterLanguageFallbackItemSwitcher = LanguageFallbackItemSwitcher.CurrentValue;
            Parallel.ForEach<IComputedIndexField>(base.Options.ComputedIndexFields, base.ParallelOptions, delegate (IComputedIndexField computedIndexField, ParallelLoopState parallelLoopState)
            {
                object fieldValue;
                try
                {
                    using (new LanguageFallbackItemSwitcher(needEnterLanguageFallbackItemSwitcher))
                    {
                        using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
                        {
                            fieldValue = computedIndexField.ComputeFieldValue(this.Indexable);
                        }
                    }
                }
                catch (Exception ex)
                {
                    CrawlingLog.Log.Warn(string.Format("Could not compute value for ComputedIndexField: {0} for indexable: {1}", computedIndexField.FieldName, this.Indexable.UniqueId), ex);
                    if (this.Settings.StopOnCrawlFieldError())
                    {
                        exceptions.Enqueue(ex);
                        parallelLoopState.Stop();
                    }
                    return;
                }
                this.AddComputedIndexField(computedIndexField, fieldValue);
            });
            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
        }

        private void AddComputedIndexField(IComputedIndexField computedIndexField, object fieldValue)
        {
            AddComputedIndexFieldMethodInfo.Invoke(this, new object[]
            {
                computedIndexField, fieldValue
            });
        }

        protected override void AddComputedIndexFieldsInSequence()
        {
            foreach (IComputedIndexField current in base.Options.ComputedIndexFields)
            {
                object fieldValue;
                try
                {
                    using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
                    {
                        fieldValue = current.ComputeFieldValue(base.Indexable);
                    }
                }
                catch (Exception exception)
                {
                    CrawlingLog.Log.Warn(string.Format("Could not compute value for ComputedIndexField: {0} for indexable: {1}", current.FieldName, base.Indexable.UniqueId), exception);
                    if (base.Settings.StopOnCrawlFieldError())
                    {
                        throw;
                    }
                    continue;
                }
                this.AddComputedIndexField(current, fieldValue);
            }
        }

    }
}