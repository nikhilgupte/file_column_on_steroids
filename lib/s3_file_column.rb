require 'right_aws'
require 'active_support'

module FileColumn # :nodoc:
  module S3FileColumnExtension
    def self.included(base) # :nodoc:
      base.extend ClassMethods
    end

    module Config
      mattr_accessor :s3_access_key_id, :s3_secret_access_key, :s3_bucket_name, :s3_distribution_url
    end

    module ClassMethods

      def s3_file_column(attr, options={})
        # create methods to move files in bulk to S3
        %w(move copy).each do |operation|
          (class << self; self; end).instance_eval do
            define_method "#{operation}_all_#{attr.to_s.pluralize}_to_s3" do |*args|
              options = args.empty? ? {} : args.first
              options.reverse_merge!(:conditions => ["#{attr} is not null and #{attr}_in_s3 = ?", false])
              recs = find(:all, options)
              count = recs.size
              time = Time.now
              elapsed = 0
              recs.each_with_index do |obj,i|
                obj.send("#{operation}_#{attr}_to_s3")
                j = i + 1
                if j % 10 == 0
                  elapsed = (Time.now - time).to_i
                  percentage_done = j.to_f * 100/count.to_f
                  estimated = (count.to_f * elapsed/j.to_f).to_i - elapsed
                  p "done #{j} out of #{count} (#{percentage_done.round}%) elapsed time: #{elapsed} estimated time: #{estimated} seconds"
                end
              end
              "Transferred #{count} files in around #{elapsed} seconds"
            end
          end
        end
        define_method "copy_#{attr}_to_s3" do
          begin
            if File.exists? send(attr)
              self.class.s3_bucket.delete_folder(send("#{attr}_web_folder_path")) unless send(attr).index("file")
              self.class._upload_to_s3(send("#{attr}_web_path", nil), send(attr))
              options = send("#{attr}_options")
              if options[:magick] && options[:magick][:versions]
                options[:magick][:versions].keys.each do |version|
                  self.class._upload_to_s3(send("#{attr}_web_path", version), send(attr, version.to_s))
                end
              end
              send("#{attr}_s3_after_copy")
              return true
            end
          rescue Rightscale::HttpConnection
            logger.error("#{self.class.name}: #{$!}")
            raise $!
          rescue
            logger.error("#{self.class.name}: Instance: #{id}: Failed to upload #{attr}: #{$!}")
            logger.error($!.backtrace.join('\n'))
          end
          return false
        end

        define_method "move_#{attr}_to_s3" do
          if (img_path = send(attr)) && File.exists?(img_path)
            if send("copy_#{attr}_to_s3")
              dir = send("#{attr}_dir")
              FileUtils.rm_rf "#{dir}"
              return true
            else
              return false
            end
          end
        end

        define_method "#{attr}_web_folder_path" do
          options = send("#{attr}_options")
          relative_path = send("#{attr}_relative_dir")
          "#{options[:base_url]}/#{relative_path}"
        end

        define_method "#{attr}_web_path" do |*args|
          version = args.to_s unless args.nil?
          options = send("#{attr}_options")
          relative_path = send("#{attr}_relative_path", version)
          "#{options[:base_url]}/#{relative_path}"
        end

        define_method "#{attr}_s3_after_copy" do
          self.class.update_all(["#{attr}_in_s3 = ?", true], ['id = ?', send('id')])
          self.reload
        end

        before_save_method = "#{attr}_before_save_s3".to_sym
        define_method before_save_method do
           #if send("#{attr}_just_uploaded?")
           if send("#{attr}_changed?")
             send("#{attr}_in_s3=", false)
           end
          true
        end
        before_save before_save_method

        after_save_method_s3 = "#{attr}_after_save_s3".to_sym
      
        define_method after_save_method_s3 do
          #if send("#{attr}_just_uploaded?")
          if send("#{attr}_changed?")
            options = send("#{attr}_options")
            if options[:s3_auto]
              send("#{options[:s3_auto]}_#{attr}_to_s3")
            end
          end
          true
        end
        after_save after_save_method_s3

      end

      def _upload_to_s3(key, file_path)
        s3_bucket.put(key, File.open(file_path), {}, 'public-read', {'Cache-Control' => 'public, max-age=31536000', 'Expires' => 10.years.from_now.httpdate})
        logger.debug("Uploaded #{file_path} as #{key}")
      end

      def s3_bucket
        @@s3_bucket ||= begin
          logger.debug("#{self.class_name}: Getting S3 Bucket...")
          s3 = RightAws::S3.new(FileColumn::S3FileColumnExtension::Config::s3_access_key_id, FileColumn::S3FileColumnExtension::Config::s3_secret_access_key)
          bucket = s3.bucket(FileColumn::S3FileColumnExtension::Config::s3_bucket_name, true, 'public-read')
        end
      end

    end
  end
end
